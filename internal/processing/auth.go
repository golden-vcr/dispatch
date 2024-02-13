package processing

import (
	"context"
	"strings"

	"github.com/golden-vcr/auth"
	"github.com/golden-vcr/schemas/core"
)

func requestServiceToken(ctx context.Context, client auth.ServiceClient, viewer *core.Viewer) (string, error) {
	return client.RequestServiceToken(ctx, auth.ServiceTokenRequest{
		Service: "dispatch",
		User: auth.UserDetails{
			Id:          viewer.TwitchUserId,
			Login:       strings.ToLower(viewer.TwitchDisplayName),
			DisplayName: viewer.TwitchDisplayName,
		},
	})
}
