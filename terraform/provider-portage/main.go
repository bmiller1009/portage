// Terraform provider for Portage (docs/architecture/spec.md §69). A thin
// CRUD wrapper over the public REST API (control_plane/repositories.py's
// update_*/delete_* functions, api/routers/*.py's PUT/DELETE endpoints) --
// no local state beyond what Terraform itself already tracks, matching
// this project's "the CLI/SDK/integrations are pure REST clients" rule
// (spec §31) applied to infrastructure-as-code too.
package main

import (
	"context"
	"log"

	tfprovider "github.com/hashicorp/terraform-plugin-framework/provider"
	"github.com/hashicorp/terraform-plugin-framework/providerserver"

	"github.com/bmiller1009/portage/terraform/provider-portage/internal/provider"
)

func main() {
	err := providerserver.Serve(
		context.Background(),
		func() tfprovider.Provider { return provider.New() },
		providerserver.ServeOpts{Address: "registry.terraform.io/bmiller1009/portage"},
	)
	if err != nil {
		log.Fatal(err)
	}
}
