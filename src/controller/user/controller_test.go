// Copyright Project Harbor Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package user

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	commonmodels "github.com/goharbor/harbor/src/common/models"
	"github.com/goharbor/harbor/src/common/security"
	"github.com/goharbor/harbor/src/common/security/local"
)

func TestOverlayAdminRoleInAuth(t *testing.T) {
	sessionUser := &commonmodels.User{
		UserID:          7,
		Username:        "alice",
		AdminRoleInAuth: true,
	}
	ctx := security.NewContext(context.Background(), local.NewSecurityContext(sessionUser))

	users := []*commonmodels.User{
		{UserID: 7, Username: "alice"},
		{UserID: 8, Username: "bob"},
		nil,
	}
	overlayAdminRoleInAuth(ctx, users)

	assert.True(t, users[0].AdminRoleInAuth)
	assert.False(t, users[1].AdminRoleInAuth)

	// No local security context: nothing is copied, including a stale value.
	users[0].AdminRoleInAuth = true
	overlayAdminRoleInAuth(context.Background(), users)
	assert.True(t, users[0].AdminRoleInAuth)

	// A session that is no longer an admin group member clears the flag
	// for that user only.
	sessionUser.AdminRoleInAuth = false
	ctx = security.NewContext(context.Background(), local.NewSecurityContext(sessionUser))
	overlayAdminRoleInAuth(ctx, users)
	assert.False(t, users[0].AdminRoleInAuth)
	assert.False(t, users[1].AdminRoleInAuth)
}
