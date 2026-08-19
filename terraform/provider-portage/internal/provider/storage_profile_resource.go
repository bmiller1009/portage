package provider

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

type storageProfileResource struct {
	client *Client
}

// ProviderType, not "provider" -- see execution_profile_resource.go's own
// comment: "provider" is a reserved Terraform resource-block
// meta-argument, confirmed live to cause a bogus provider-not-found
// error rather than reaching this provider's own logic.
type storageProfileModel struct {
	Name                types.String `tfsdk:"name"`
	ProviderType        types.String `tfsdk:"provider_type"`
	Config              types.String `tfsdk:"config"`
	CredentialReference types.String `tfsdk:"credential_reference"`
}

type storageProfileAPI struct {
	Name                string          `json:"name,omitempty"`
	Provider            string          `json:"provider"`
	Config              json.RawMessage `json:"config"`
	CredentialReference json.RawMessage `json:"credential_reference"`
}

func NewStorageProfileResource() resource.Resource {
	return &storageProfileResource{}
}

func (r *storageProfileResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_storage_profile"
}

func (r *storageProfileResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "A storage profile (docs/architecture/spec.md §48) -- provider-specific config for an S3/VAST/ADLS storage target. credential_reference never holds a raw secret (spec §35), only a reference to resolve one.",
		Attributes: map[string]schema.Attribute{
			"name": schema.StringAttribute{
				Required:      true,
				Description:   "Unique name -- immutable, changing it replaces the resource.",
				PlanModifiers: []planmodifier.String{stringplanmodifier.RequiresReplace()},
			},
			"provider_type": schema.StringAttribute{
				Required:    true,
				Description: `"s3", "vast", or "adls". Named provider_type (not "provider") -- "provider" is a reserved Terraform meta-argument.`,
			},
			"config": schema.StringAttribute{
				Required:    true,
				Description: "Provider-specific config as a JSON-encoded string.",
			},
			"credential_reference": schema.StringAttribute{
				Required:    true,
				Description: "A reference to resolve credentials from, as a JSON-encoded string, e.g. jsonencode({provider = \"env\", reference = \"PORTAGE_MINIO\"}).",
			},
		},
	}
}

func (r *storageProfileResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	if req.ProviderData == nil {
		return
	}
	client, ok := req.ProviderData.(*Client)
	if !ok {
		resp.Diagnostics.AddError("Unexpected Resource Configure Type", fmt.Sprintf("expected *Client, got: %T", req.ProviderData))
		return
	}
	r.client = client
}

func (r *storageProfileResource) applyState(model *storageProfileModel, out storageProfileAPI) {
	model.Name = types.StringValue(out.Name)
	model.ProviderType = types.StringValue(out.Provider)
	model.Config = types.StringValue(string(out.Config))
	model.CredentialReference = types.StringValue(string(out.CredentialReference))
}

func (r *storageProfileResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan storageProfileModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	body := storageProfileAPI{
		Name:                plan.Name.ValueString(),
		Provider:            plan.ProviderType.ValueString(),
		Config:              json.RawMessage(plan.Config.ValueString()),
		CredentialReference: json.RawMessage(plan.CredentialReference.ValueString()),
	}
	var out storageProfileAPI
	if err := r.client.Post(ctx, "/v1/storage-profiles", body, &out); err != nil {
		resp.Diagnostics.AddError("Error creating storage profile", err.Error())
		return
	}

	r.applyState(&plan, out)
	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

func (r *storageProfileResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state storageProfileModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	var out storageProfileAPI
	err := r.client.Get(ctx, "/v1/storage-profiles/"+state.Name.ValueString(), &out)
	if apiErr, ok := err.(*APIError); ok && apiErr.NotFound() {
		resp.State.RemoveResource(ctx)
		return
	}
	if err != nil {
		resp.Diagnostics.AddError("Error reading storage profile", err.Error())
		return
	}

	r.applyState(&state, out)
	resp.Diagnostics.Append(resp.State.Set(ctx, &state)...)
}

func (r *storageProfileResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan storageProfileModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	body := storageProfileAPI{
		Provider:            plan.ProviderType.ValueString(),
		Config:              json.RawMessage(plan.Config.ValueString()),
		CredentialReference: json.RawMessage(plan.CredentialReference.ValueString()),
	}
	var out storageProfileAPI
	if err := r.client.Put(ctx, "/v1/storage-profiles/"+plan.Name.ValueString(), body, &out); err != nil {
		resp.Diagnostics.AddError("Error updating storage profile", err.Error())
		return
	}

	r.applyState(&plan, out)
	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

func (r *storageProfileResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var state storageProfileModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	if err := r.client.Delete(ctx, "/v1/storage-profiles/"+state.Name.ValueString()); err != nil {
		resp.Diagnostics.AddError("Error deleting storage profile", err.Error())
	}
}
