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

type workloadResource struct {
	client *Client
}

type workloadModel struct {
	Name       types.String `tfsdk:"name"`
	Version    types.String `tfsdk:"version"`
	Definition types.String `tfsdk:"definition"`
}

// workloadOut mirrors api/schemas.py's WorkloadDefinitionOut -- GET/PUT
// responses wrap the portable workload under a "definition" key,
// alongside the name/version the record is addressed by.
type workloadOut struct {
	Name       string          `json:"name"`
	Version    string          `json:"version"`
	Definition json.RawMessage `json:"definition"`
}

func NewWorkloadResource() resource.Resource {
	return &workloadResource{}
}

func (r *workloadResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_workload"
}

func (r *workloadResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "A portable workload definition (docs/architecture/spec.md §7). `definition` is the full workload body as a JSON-encoded string (jsonencode(...) over apiVersion/kind/metadata/runtime/application/datasets/resources/execution) -- the same document POST /v1/workloads accepts directly, not a Terraform-native re-modeling of every nested field.",
		Attributes: map[string]schema.Attribute{
			"name": schema.StringAttribute{
				Required:      true,
				Description:   "Workload name -- immutable, changing it replaces the resource (register a new workload rather than renaming one in place).",
				PlanModifiers: []planmodifier.String{stringplanmodifier.RequiresReplace()},
			},
			"version": schema.StringAttribute{
				Required:      true,
				Description:   "Workload version -- immutable, changing it replaces the resource (a new version is a new resource, not an update to an existing one).",
				PlanModifiers: []planmodifier.String{stringplanmodifier.RequiresReplace()},
			},
			"definition": schema.StringAttribute{
				Required:    true,
				Description: "The full portable workload definition as a JSON-encoded string.",
			},
		},
	}
}

func (r *workloadResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

func (r *workloadResource) applyState(model *workloadModel, out workloadOut) {
	model.Name = types.StringValue(out.Name)
	model.Version = types.StringValue(out.Version)
	model.Definition = types.StringValue(string(out.Definition))
}

func (r *workloadResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan workloadModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	// POST /v1/workloads takes the workload body directly (spec.SparkWorkload),
	// not wrapped under a "definition" key -- unlike the GET/PUT response shape.
	var out workloadOut
	if err := r.client.Post(ctx, "/v1/workloads", json.RawMessage(plan.Definition.ValueString()), &out); err != nil {
		resp.Diagnostics.AddError("Error creating workload", err.Error())
		return
	}

	r.applyState(&plan, out)
	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

func (r *workloadResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state workloadModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	var out workloadOut
	path := fmt.Sprintf("/v1/workloads/%s?version=%s", state.Name.ValueString(), state.Version.ValueString())
	err := r.client.Get(ctx, path, &out)
	if apiErr, ok := err.(*APIError); ok && apiErr.NotFound() {
		resp.State.RemoveResource(ctx)
		return
	}
	if err != nil {
		resp.Diagnostics.AddError("Error reading workload", err.Error())
		return
	}

	r.applyState(&state, out)
	resp.Diagnostics.Append(resp.State.Set(ctx, &state)...)
}

func (r *workloadResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan workloadModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	var out workloadOut
	path := fmt.Sprintf("/v1/workloads/%s?version=%s", plan.Name.ValueString(), plan.Version.ValueString())
	if err := r.client.Put(ctx, path, json.RawMessage(plan.Definition.ValueString()), &out); err != nil {
		resp.Diagnostics.AddError("Error updating workload", err.Error())
		return
	}

	r.applyState(&plan, out)
	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

func (r *workloadResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var state workloadModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	path := fmt.Sprintf("/v1/workloads/%s?version=%s", state.Name.ValueString(), state.Version.ValueString())
	if err := r.client.Delete(ctx, path); err != nil {
		resp.Diagnostics.AddError("Error deleting workload", err.Error())
	}
}
