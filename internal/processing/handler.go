package processing

import (
	"context"
	"fmt"
	"strings"

	"github.com/golden-vcr/auth"
	"github.com/golden-vcr/ledger"
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

func NewHandler(authServiceClient auth.ServiceClient, ledgerClient ledger.Client, onscreenEventsProducer rmq.Producer, generationRequestsProducer rmq.Producer) Handler {
	return &handler{
		authServiceClient:          authServiceClient,
		ledgerClient:               ledgerClient,
		onscreenEventsProducer:     onscreenEventsProducer,
		generationRequestsProducer: generationRequestsProducer,
	}
}

type handler struct {
	authServiceClient          auth.ServiceClient
	ledgerClient               ledger.Client
	onscreenEventsProducer     rmq.Producer
	generationRequestsProducer rmq.Producer
}

func (h *handler) Handle(ctx context.Context, logger *slog.Logger, ev *etwitch.Event) error {
	switch ev.Type {
	case etwitch.EventTypeViewerFollowed:
		return h.handleViewerFollowed(ctx, logger, ev.Viewer)
	case etwitch.EventTypeViewerCheered:
		viewerOrAnonymous := ev.Viewer
		return h.handleViewerCheered(ctx, logger, viewerOrAnonymous, ev.Payload.ViewerCheered)
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

func (h *handler) handleViewerFollowed(ctx context.Context, logger *slog.Logger, viewer *etwitch.Viewer) error {
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

func (h *handler) handleViewerCheered(ctx context.Context, logger *slog.Logger, viewerOrAnonymous *etwitch.Viewer, payload *etwitch.PayloadViewerCheered) error {
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

	// Parse our message text to see if it represents a request to generate an alert,
	// and produce a new message to the 'generation-requests' queue if so
	if payload.NumBits >= 200 && viewerOrAnonymous != nil {
		ghostPrefix := "ghost of "
		ghostSubject := ""
		ghostPrefixPos := strings.Index(strings.ToLower(payload.Message), ghostPrefix)
		if ghostPrefixPos >= 0 {
			ghostSubject = strings.TrimSpace(payload.Message[ghostPrefixPos+len(ghostPrefix):])
		}
		if ghostSubject != "" {
			err := h.produceGenerationRequest(ctx, logger, genreq.Request{
				Type:   genreq.RequestTypeImage,
				Viewer: *viewerOrAnonymous,
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

func (h *handler) handleViewerSubscribed(ctx context.Context, logger *slog.Logger, viewer *etwitch.Viewer, payload *etwitch.PayloadViewerSubscribed) error {
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

func (h *handler) handleViewerResubscribed(ctx context.Context, logger *slog.Logger, viewer *etwitch.Viewer, payload *etwitch.PayloadViewerResubscribed) error {
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

func (h *handler) handleViewerReceivedGiftSub(ctx context.Context, logger *slog.Logger, viewer *etwitch.Viewer, payload *etwitch.PayloadViewerReceivedGiftSub) error {
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

func (h *handler) handleViewerGiftedSubs(ctx context.Context, logger *slog.Logger, viewerOrAnonymous *etwitch.Viewer, payload *etwitch.PayloadViewerGiftedSubs) error {
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
