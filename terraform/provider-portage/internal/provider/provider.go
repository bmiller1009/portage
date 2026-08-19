package provider

import (
	"context"
	"net/http"
	"os"

	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/provider"
	"github.com/hashicorp/terraform-plugin-framework/provider/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

type portageProvider struct{}

type portageProviderModel struct {
	APIURL types.String `tfsdk:"api_url"`
}

func New() provider.Provider {
	return &portageProvider{}
}

func (p *portageProvider) Metadata(_ context.Context, _ provider.MetadataRequest, resp *provider.MetadataResponse) {
	resp.TypeName = "portage"
}

func (p *portageProvider) Schema(_ context.Context, _ provider.SchemaRequest, resp *provider.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Manages Portage (docs/architecture/spec.md) control-plane config resources -- environments, execution profiles, storage profiles, and workload definitions -- as infrastructure-as-code, wrapping the same public REST API the CLI uses.",
		Attributes: map[string]schema.Attribute{
			"api_url": schema.StringAttribute{
				Optional:    true,
				Description: "Base URL of the Portage control-plane API. Defaults to $PORTAGE_API_URL, matching the CLI's own convention, then http://localhost:8000.",
			},
		},
	}
}

func (p *portageProvider) Configure(ctx context.Context, req provider.ConfigureRequest, resp *provider.ConfigureResponse) {
	var config portageProviderModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &config)...)
	if resp.Diagnostics.HasError() {
		return
	}

	apiURL := config.APIURL.ValueString()
	if apiURL == "" {
		apiURL = os.Getenv("PORTAGE_API_URL")
	}
	if apiURL == "" {
		apiURL = "http://localhost:8000"
	}

	client := &Client{BaseURL: apiURL, HTTPClient: http.DefaultClient}
	resp.ResourceData = client
}

func (p *portageProvider) Resources(_ context.Context) []func() resource.Resource {
	return []func() resource.Resource{
		NewEnvironmentResource,
		NewExecutionProfileResource,
		NewStorageProfileResource,
		NewWorkloadResource,
	}
}

func (p *portageProvider) DataSources(_ context.Context) []func() datasource.DataSource {
	return nil
}
