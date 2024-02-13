package processing

import (
	"context"
	"fmt"
	"strings"

	"github.com/golden-vcr/auth"
	"github.com/golden-vcr/broadcasts"
	"github.com/golden-vcr/ledger"
	"github.com/golden-vcr/schemas/core"
	genreq "github.com/golden-vcr/schemas/generation-requests"
	e "github.com/golden-vcr/schemas/onscreen-events"
	etwitch "github.com/golden-vcr/schemas/twitch-events"
	"github.com/golden-vcr/server-common/rmq"
	"golang.org/x/exp/slog"
)

const BasePointsForSubscription = 600
const BasePointsForGiftSub = 200

type Handler interface {
	Handle(ctx context.Context, logger *slog.Logger, ev *etwitch.Event) error
}

func NewHandler(authServiceClient auth.ServiceClient, ledgerClient ledger.Client, broadcastsClient broadcasts.Client, onscreenEventsProducer rmq.Producer, generationRequestsProducer rmq.Producer) Handler {
	return &handler{
		authServiceClient:          authServiceClient,
		ledgerClient:               ledgerClient,
		broadcastsClient:           broadcastsClient,
		onscreenEventsProducer:     onscreenEventsProducer,
		generationRequestsProducer: generationRequestsProducer,
	}
}

type handler struct {
	authServiceClient          auth.ServiceClient
	ledgerClient               ledger.Client
	broadcastsClient           broadcasts.Client
	onscreenEventsProducer     rmq.Producer
	generationRequestsProducer rmq.Producer
}

func (h *handler) Handle(ctx context.Context, logger *slog.Logger, ev *etwitch.Event) error {
	switch ev.Type {
	case etwitch.EventTypeViewerFollowed:
		return h.handleViewerFollowed(ctx, logger, ev.Viewer)
	case etwitch.EventTypeViewerRaided:
		return h.handleViewerRaided(ctx, logger, ev.Viewer, ev.Payload.ViewerRaided)
	case etwitch.EventTypeViewerCheered:
		viewerOrAnonymous := ev.Viewer
		return h.handleViewerCheered(ctx, logger, viewerOrAnonymous, ev.Payload.ViewerCheered)
	case etwitch.EventTypeViewerRedeemedFunPoints:
		return h.handleViewerRedeemedFunPoints(ctx, logger, ev.Viewer, ev.Payload.ViewerRedeemedFunPoints.NumPoints, ev.Payload.ViewerRedeemedFunPoints.Message)
	case etwitch.EventTypeViewerSubscribed:
		return h.handleViewerSubscribed(ctx, logger, ev.Viewer, ev.Payload.ViewerSubscribed)
	case etwitch.EventTypeViewerResubscribed:
		return h.handleViewerResubscribed(ctx, logger, ev.Viewer, ev.Payload.ViewerResubscribed)
	case etwitch.EventTypeViewerReceivedGiftSub:
		return h.handleViewerReceivedGiftSub(ctx, logger, ev.Viewer, ev.Payload.ViewerReceivedGiftSub)
	case etwitch.EventTypeViewerGiftedSubs:
		viewerOrAnonymous := ev.Viewer
		return h.handleViewerGiftedSubs(ctx, logger, viewerOrAnonymous, ev.Payload.ViewerGiftedSubs)
	}
	return nil
}

func (h *handler) handleViewerFollowed(ctx context.Context, logger *slog.Logger, viewer *core.Viewer) error {
	// Generate an alert indicating that this viewer is now following the channel
	err := h.produceOnscreenEvent(ctx, logger, e.Event{
		Type: e.EventTypeToast,
		Payload: e.Payload{
			Toast: &e.PayloadToast{
				Type:   e.ToastTypeFollowed,
				Viewer: viewer,
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to produce onscreen event: %w", err)
	}
	return nil
}

func (h *handler) handleViewerRaided(ctx context.Context, logger *slog.Logger, viewer *core.Viewer, payload *etwitch.PayloadViewerRaided) error {
	// Generate an alert indicating that this viewer has raided
	err := h.produceOnscreenEvent(ctx, logger, e.Event{
		Type: e.EventTypeToast,
		Payload: e.Payload{
			Toast: &e.PayloadToast{
				Type:   e.ToastTypeRaided,
				Viewer: viewer,
				Data: &e.ToastData{
					Raided: &e.ToastDataRaided{
						NumViewers: payload.NumRaiders,
					},
				},
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to produce onscreen event: %w", err)
	}
	return nil
}

func (h *handler) handleViewerCheered(ctx context.Context, logger *slog.Logger, viewerOrAnonymous *core.Viewer, payload *etwitch.PayloadViewerCheered) error {
	// If not anonymous, credit fun points to the viewer's balance
	if viewerOrAnonymous != nil {
		accessToken, err := requestServiceToken(ctx, h.authServiceClient, viewerOrAnonymous)
		if err != nil {
			return fmt.Errorf("failed to get service token: %w", err)
		}
		flowId, err := h.ledgerClient.RequestCreditFromCheer(ctx, accessToken, payload.NumBits, payload.Message)
		if err != nil {
			return fmt.Errorf("failed to request credit from cheer: %w", err)
		}
		logger.Info("Credited fun points to viewer balance",
			"flowType", "cheer",
			"flowId", flowId,
		)
	}

	// Generate an alert to display the user's cheer
	err := h.produceOnscreenEvent(ctx, logger, e.Event{
		Type: e.EventTypeToast,
		Payload: e.Payload{
			Toast: &e.PayloadToast{
				Type:   e.ToastTypeCheered,
				Viewer: viewerOrAnonymous,
				Data: &e.ToastData{
					Cheered: &e.ToastDataCheered{
						NumBits: payload.NumBits,
						Message: payload.Message,
					},
				},
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to produce onscreen event: %w", err)
	}

	// If we have a viewer, parse their message to determine if we should immediately
	// redeem some or all of the fun points they were just credited in order to produce
	// an alert
	if viewerOrAnonymous != nil {
		viewer := viewerOrAnonymous
		return h.handleViewerRedeemedFunPoints(ctx, logger, viewer, payload.NumBits, payload.Message)
	}
	return nil
}

func (h *handler) handleViewerRedeemedFunPoints(ctx context.Context, logger *slog.Logger, viewer *core.Viewer, numPoints int, message string) error {
	// Parse our message text to see if it represents a request to generate an alert,
	// and produce a new message to the 'generation-requests' queue if so
	if numPoints >= 200 {
		ghostPrefix := "ghost of "
		ghostSubject := ""
		ghostPrefixPos := strings.Index(strings.ToLower(message), ghostPrefix)
		if ghostPrefixPos >= 0 {
			ghostSubject = strings.TrimSpace(message[ghostPrefixPos+len(ghostPrefix):])
		}
		if ghostSubject != "" {
			err := h.produceGenerationRequest(ctx, logger, genreq.Request{
				Type:   genreq.RequestTypeImage,
				Viewer: *viewer,
				State:  h.broadcastsClient.GetState(),
				Payload: genreq.Payload{
					Image: &genreq.PayloadImage{
						Style: genreq.ImageStyleGhost,
						Inputs: genreq.ImageInputs{
							Ghost: &genreq.ImageInputsGhost{
								Subject: ghostSubject,
							},
						},
					},
				},
			})
			if err != nil {
				return fmt.Errorf("failed to produce generation request: %w", err)
			}
		}
	}
	return nil
}

func (h *handler) handleViewerSubscribed(ctx context.Context, logger *slog.Logger, viewer *core.Viewer, payload *etwitch.PayloadViewerSubscribed) error {
	// Credit fun points to the viewer's balance
	accessToken, err := requestServiceToken(ctx, h.authServiceClient, viewer)
	if err != nil {
		return fmt.Errorf("failed to get service token: %w", err)
	}
	isInitial := true
	isGift := false
	flowId, err := h.ledgerClient.RequestCreditFromSubscription(ctx, accessToken, BasePointsForSubscription, isInitial, isGift, "", float64(payload.CreditMultiplier))
	if err != nil {
		return fmt.Errorf("failed to request credit from subscription: %w", err)
	}
	logger.Info("Credited fun points to viewer balance",
		"flowType", "subscription",
		"flowId", flowId,
	)

	// Generate an alert indicating that the viewer is now subscribed to the channel
	err = h.produceOnscreenEvent(ctx, logger, e.Event{
		Type: e.EventTypeToast,
		Payload: e.Payload{
			Toast: &e.PayloadToast{
				Type:   e.ToastTypeSubscribed,
				Viewer: viewer,
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to produce onscreen event: %w", err)
	}
	return nil
}

func (h *handler) handleViewerResubscribed(ctx context.Context, logger *slog.Logger, viewer *core.Viewer, payload *etwitch.PayloadViewerResubscribed) error {
	// Credit fun points to the viewer's balance
	accessToken, err := requestServiceToken(ctx, h.authServiceClient, viewer)
	if err != nil {
		return fmt.Errorf("failed to get service token: %w", err)
	}
	isInitial := false
	isGift := false
	flowId, err := h.ledgerClient.RequestCreditFromSubscription(ctx, accessToken, BasePointsForSubscription, isInitial, isGift, payload.Message, float64(payload.CreditMultiplier))
	if err != nil {
		return fmt.Errorf("failed to request credit from subscription: %w", err)
	}
	logger.Info("Credited fun points to viewer balance",
		"flowType", "subscription",
		"flowId", flowId,
	)

	// Generate an alert to display the viewer's resub message
	err = h.produceOnscreenEvent(ctx, logger, e.Event{
		Type: e.EventTypeToast,
		Payload: e.Payload{
			Toast: &e.PayloadToast{
				Type:   e.ToastTypeResubscribed,
				Viewer: viewer,
				Data: &e.ToastData{
					Resubscribed: &e.ToastDataResubscribed{
						NumCumulativeMonths: payload.NumCumulativeMonths,
						Message:             payload.Message,
					},
				},
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to produce onscreen event: %w", err)
	}
	return nil
}

func (h *handler) handleViewerReceivedGiftSub(ctx context.Context, logger *slog.Logger, viewer *core.Viewer, payload *etwitch.PayloadViewerReceivedGiftSub) error {
	// Credit fun points to the recipient's balance, same as if they'd subscribed
	accessToken, err := requestServiceToken(ctx, h.authServiceClient, viewer)
	if err != nil {
		return fmt.Errorf("failed to get service token: %w", err)
	}
	isInitial := true
	isGift := true
	flowId, err := h.ledgerClient.RequestCreditFromSubscription(ctx, accessToken, BasePointsForSubscription, isInitial, isGift, "", float64(payload.CreditMultiplier))
	if err != nil {
		return fmt.Errorf("failed to request credit from subscription: %w", err)
	}
	logger.Info("Credited fun points to viewer balance",
		"flowType", "subscription",
		"flowId", flowId,
	)
	return nil
}

func (h *handler) handleViewerGiftedSubs(ctx context.Context, logger *slog.Logger, viewerOrAnonymous *core.Viewer, payload *etwitch.PayloadViewerGiftedSubs) error {
	// If not anonymous, credit fun points to the gifter's balance
	if viewerOrAnonymous != nil {
		accessToken, err := requestServiceToken(ctx, h.authServiceClient, viewerOrAnonymous)
		if err != nil {
			return fmt.Errorf("failed to get service token: %w", err)
		}
		flowId, err := h.ledgerClient.RequestCreditFromGiftSub(ctx, accessToken, BasePointsForGiftSub, payload.NumSubscriptions, float64(payload.CreditMultiplier))
		if err != nil {
			return fmt.Errorf("failed to request credit from gift sub grant: %w", err)
		}
		logger.Info("Credited fun points to viewer balance",
			"flowType", "gift-sub",
			"flowId", flowId,
		)
	}

	// Generate an alert to indicate that gift subs have been given out
	err := h.produceOnscreenEvent(ctx, logger, e.Event{
		Type: e.EventTypeToast,
		Payload: e.Payload{
			Toast: &e.PayloadToast{
				Type:   e.ToastTypeGiftedSubs,
				Viewer: viewerOrAnonymous,
				Data: &e.ToastData{
					GiftedSubs: &e.ToastDataGiftedSubs{
						NumSubscriptions: payload.NumSubscriptions,
					},
				},
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to produce onscreen event: %w", err)
	}
	return nil
}
