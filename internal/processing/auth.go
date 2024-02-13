package processing

import (
	"context"
	"fmt"
	"strings"

	"github.com/golden-vcr/auth"
	"github.com/golden-vcr/schemas/core"
)

func requestServiceToken(ctx context.Context, client auth.ServiceClient, viewer *core.Viewer) (string, error) {
	// TEMP: Only allow this service to update the state of a single test user for now
	if viewer.TwitchUserId != "90790024" {
		return "", fmt.Errorf("testing is restricted to events instigated by user 90790024")
	}

	return client.RequestServiceToken(ctx, auth.ServiceTokenRequest{
		Service: "dispatch",
		User: auth.UserDetails{
			Id:          viewer.TwitchUserId,
			Login:       strings.ToLower(viewer.TwitchDisplayName),
			DisplayName: viewer.TwitchDisplayName,
		},
	})
}
