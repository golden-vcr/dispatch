package processing

import (
	"context"
	"encoding/json"

	genreq "github.com/golden-vcr/schemas/generation-requests"
	eonscreen "github.com/golden-vcr/schemas/onscreen-events"
)

func (h *handler) produceGenerationRequest(ctx context.Context, req genreq.Request) error {
	data, err := json.Marshal(req)
	if err != nil {
		return err
	}
	return h.generationRequestsProducer.Send(ctx, data)
}

func (h *handler) produceOnscreenEvent(ctx context.Context, ev eonscreen.Event) error {
	data, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	return h.onscreenEventsProducer.Send(ctx, data)
}
