package provider_test

import (
	"github.com/hashicorp/terraform-plugin-framework/providerserver"
	"github.com/hashicorp/terraform-plugin-go/tfprotov6"

	"github.com/bmiller1009/portage/terraform/provider-portage/internal/provider"
)

// testAccProtoV6ProviderFactories wires the real provider implementation
// into terraform-plugin-testing's harness -- the standard boilerplate
// every terraform-plugin-framework acceptance test needs.
var testAccProtoV6ProviderFactories = map[string]func() (tfprotov6.ProviderServer, error){
	"portage": providerserver.NewProtocol6WithError(provider.New()),
}
