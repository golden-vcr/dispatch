package processing

import (
	"context"
	"encoding/json"

	genreq "github.com/golden-vcr/schemas/generation-requests"
	eonscreen "github.com/golden-vcr/schemas/onscreen-events"
	"golang.org/x/exp/slog"
)

func (h *handler) produceGenerationRequest(ctx context.Context, logger *slog.Logger, req genreq.Request) error {
	logger = logger.With("generationRequest", req)
	data, err := json.Marshal(req)
	if err != nil {
		return err
	}
	err = h.generationRequestsProducer.Send(ctx, data)
	if err != nil {
		logger.Error("Failed to produce to generation-requests")
	} else {
		logger.Info("Produced to generation-requests")
	}
	return err
}

func (h *handler) produceOnscreenEvent(ctx context.Context, logger *slog.Logger, ev eonscreen.Event) error {
	logger = logger.With("onscreenEvent", ev)
	data, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	err = h.onscreenEventsProducer.Send(ctx, data)
	if err != nil {
		logger.Error("Failed to produce to onscreen-events")
	} else {
		logger.Info("Produced to onscreen-events")
	}
	return err
}
