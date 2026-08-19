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

type executionProfileResource struct {
	client *Client
}

// ProviderType (not "provider") -- "provider" is a reserved meta-argument
// at the resource-block level in Terraform's own language (selects which
// provider *instance* handles a resource, e.g. provider = aws.west);
// confirmed live that defining a schema attribute literally named
// "provider" makes Terraform silently misparse execution_profile blocks
// as referencing an unrelated provider named after the attribute's own
// value (e.g. "kubernetes"), producing a bogus
// "hashicorp/kubernetes provider not found" error instead of ever
// reaching this provider's own logic. The wire-level JSON field stays
// "provider" (`json:"provider"`) -- only the Terraform-facing schema
// name changes.
type executionProfileModel struct {
	Name         types.String `tfsdk:"name"`
	ProviderType types.String `tfsdk:"provider_type"`
	Config       types.String `tfsdk:"config"`
}

type executionProfileAPI struct {
	Name     string          `json:"name,omitempty"`
	Provider string          `json:"provider"`
	Config   json.RawMessage `json:"config"`
}

func NewExecutionProfileResource() resource.Resource {
	return &executionProfileResource{}
}

func (r *executionProfileResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_execution_profile"
}

func (r *executionProfileResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "An execution profile (docs/architecture/spec.md §14) -- provider-specific config for a Kubernetes or Databricks execution target.",
		Attributes: map[string]schema.Attribute{
			"name": schema.StringAttribute{
				Required:      true,
				Description:   "Unique name -- immutable, changing it replaces the resource.",
				PlanModifiers: []planmodifier.String{stringplanmodifier.RequiresReplace()},
			},
			"provider_type": schema.StringAttribute{
				Required:    true,
				Description: `"kubernetes" or "databricks". Named provider_type (not "provider") -- "provider" is a reserved Terraform meta-argument.`,
			},
			"config": schema.StringAttribute{
				Required:    true,
				Description: "Provider-specific config as a JSON-encoded string, e.g. jsonencode({namespace = \"default\"}).",
			},
		},
	}
}

func (r *executionProfileResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

func (r *executionProfileResource) applyState(model *executionProfileModel, out executionProfileAPI) {
	model.Name = types.StringValue(out.Name)
	model.ProviderType = types.StringValue(out.Provider)
	model.Config = types.StringValue(string(out.Config))
}

func (r *executionProfileResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan executionProfileModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	body := executionProfileAPI{
		Name:     plan.Name.ValueString(),
		Provider: plan.ProviderType.ValueString(),
		Config:   json.RawMessage(plan.Config.ValueString()),
	}
	var out executionProfileAPI
	if err := r.client.Post(ctx, "/v1/execution-profiles", body, &out); err != nil {
		resp.Diagnostics.AddError("Error creating execution profile", err.Error())
		return
	}

	r.applyState(&plan, out)
	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

func (r *executionProfileResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state executionProfileModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	var out executionProfileAPI
	err := r.client.Get(ctx, "/v1/execution-profiles/"+state.Name.ValueString(), &out)
	if apiErr, ok := err.(*APIError); ok && apiErr.NotFound() {
		resp.State.RemoveResource(ctx)
		return
	}
	if err != nil {
		resp.Diagnostics.AddError("Error reading execution profile", err.Error())
		return
	}

	r.applyState(&state, out)
	resp.Diagnostics.Append(resp.State.Set(ctx, &state)...)
}

func (r *executionProfileResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan executionProfileModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	body := executionProfileAPI{Provider: plan.ProviderType.ValueString(), Config: json.RawMessage(plan.Config.ValueString())}
	var out executionProfileAPI
	if err := r.client.Put(ctx, "/v1/execution-profiles/"+plan.Name.ValueString(), body, &out); err != nil {
		resp.Diagnostics.AddError("Error updating execution profile", err.Error())
		return
	}

	r.applyState(&plan, out)
	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

func (r *executionProfileResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var state executionProfileModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	if err := r.client.Delete(ctx, "/v1/execution-profiles/"+state.Name.ValueString()); err != nil {
		resp.Diagnostics.AddError("Error deleting execution profile", err.Error())
	}
}
