package provider

import (
	"context"
	"fmt"

	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

type environmentResource struct {
	client *Client
}

type environmentModel struct {
	Name                 types.String `tfsdk:"name"`
	ExecutionProvider    types.String `tfsdk:"execution_provider"`
	ExecutionProfileName types.String `tfsdk:"execution_profile_name"`
	StorageProvider      types.String `tfsdk:"storage_provider"`
	StorageProfileName   types.String `tfsdk:"storage_profile_name"`
}

type environmentAPI struct {
	Name                 string `json:"name,omitempty"`
	ExecutionProvider    string `json:"execution_provider"`
	ExecutionProfileName string `json:"execution_profile_name"`
	StorageProvider      string `json:"storage_provider"`
	StorageProfileName   string `json:"storage_profile_name"`
}

func NewEnvironmentResource() resource.Resource {
	return &environmentResource{}
}

func (r *environmentResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_environment"
}

func (r *environmentResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "An environment (docs/architecture/spec.md §13) -- a named pairing of an execution profile and a storage profile that a portable workload runs against.",
		Attributes: map[string]schema.Attribute{
			"name": schema.StringAttribute{
				Required:      true,
				Description:   "Unique name -- immutable, changing it replaces the resource.",
				PlanModifiers: []planmodifier.String{stringplanmodifier.RequiresReplace()},
			},
			"execution_provider": schema.StringAttribute{
				Required:    true,
				Description: `"kubernetes" or "databricks" -- must match the referenced execution profile's own provider.`,
			},
			"execution_profile_name": schema.StringAttribute{
				Required:    true,
				Description: "Name of an existing portage_execution_profile.",
			},
			"storage_provider": schema.StringAttribute{
				Required:    true,
				Description: `"s3", "vast", or "adls" -- must match the referenced storage profile's own provider.`,
			},
			"storage_profile_name": schema.StringAttribute{
				Required:    true,
				Description: "Name of an existing portage_storage_profile.",
			},
		},
	}
}

func (r *environmentResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

func (r *environmentResource) applyState(model *environmentModel, out environmentAPI) {
	model.Name = types.StringValue(out.Name)
	model.ExecutionProvider = types.StringValue(out.ExecutionProvider)
	model.ExecutionProfileName = types.StringValue(out.ExecutionProfileName)
	model.StorageProvider = types.StringValue(out.StorageProvider)
	model.StorageProfileName = types.StringValue(out.StorageProfileName)
}

func (r *environmentResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan environmentModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	body := environmentAPI{
		Name:                 plan.Name.ValueString(),
		ExecutionProvider:    plan.ExecutionProvider.ValueString(),
		ExecutionProfileName: plan.ExecutionProfileName.ValueString(),
		StorageProvider:      plan.StorageProvider.ValueString(),
		StorageProfileName:   plan.StorageProfileName.ValueString(),
	}
	var out environmentAPI
	if err := r.client.Post(ctx, "/v1/environments", body, &out); err != nil {
		resp.Diagnostics.AddError("Error creating environment", err.Error())
		return
	}

	r.applyState(&plan, out)
	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

func (r *environmentResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state environmentModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	var out environmentAPI
	err := r.client.Get(ctx, "/v1/environments/"+state.Name.ValueString(), &out)
	if apiErr, ok := err.(*APIError); ok && apiErr.NotFound() {
		resp.State.RemoveResource(ctx)
		return
	}
	if err != nil {
		resp.Diagnostics.AddError("Error reading environment", err.Error())
		return
	}

	r.applyState(&state, out)
	resp.Diagnostics.Append(resp.State.Set(ctx, &state)...)
}

func (r *environmentResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan environmentModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	body := environmentAPI{
		ExecutionProvider:    plan.ExecutionProvider.ValueString(),
		ExecutionProfileName: plan.ExecutionProfileName.ValueString(),
		StorageProvider:      plan.StorageProvider.ValueString(),
		StorageProfileName:   plan.StorageProfileName.ValueString(),
	}
	var out environmentAPI
	if err := r.client.Put(ctx, "/v1/environments/"+plan.Name.ValueString(), body, &out); err != nil {
		resp.Diagnostics.AddError("Error updating environment", err.Error())
		return
	}

	r.applyState(&plan, out)
	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

func (r *environmentResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var state environmentModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	if err := r.client.Delete(ctx, "/v1/environments/"+state.Name.ValueString()); err != nil {
		resp.Diagnostics.AddError("Error deleting environment", err.Error())
	}
}
