package provider_test

// Acceptance test for portage_execution_profile -- real HTTP against a
// fake httptest.Server standing in for the Portage API (never the real
// control plane), using terraform-plugin-testing's standard resource.Test
// harness. Representative of the pattern for all four resources, not
// exhaustive per-resource coverage -- each resource shares the identical
// CRUD shape this test already exercises against the one real structural
// bug found live (the "provider" reserved-meta-argument collision, see
// execution_profile_resource.go's own comment).
//
// Not run in CI by default (needs TF_ACC=1 and a real terraform binary
// on PATH, same opt-in-local-check status as tests/integration/ needing
// a live Postgres):
//   TF_ACC=1 go test ./internal/provider/...

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
)

type fakeExecutionProfile struct {
	Name     string          `json:"name"`
	Provider string          `json:"provider"`
	Config   json.RawMessage `json:"config"`
}

func newFakePortageServer(t *testing.T) *httptest.Server {
	t.Helper()
	profiles := map[string]fakeExecutionProfile{}

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/execution-profiles", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			var p fakeExecutionProfile
			_ = json.NewDecoder(r.Body).Decode(&p)
			profiles[p.Name] = p
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(p)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/v1/execution-profiles/", func(w http.ResponseWriter, r *http.Request) {
		name := r.URL.Path[len("/v1/execution-profiles/"):]
		switch r.Method {
		case http.MethodGet:
			p, ok := profiles[name]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte(`{"detail":"not found"}`))
				return
			}
			_ = json.NewEncoder(w).Encode(p)
		case http.MethodPut:
			var p fakeExecutionProfile
			_ = json.NewDecoder(r.Body).Decode(&p)
			p.Name = name
			profiles[name] = p
			_ = json.NewEncoder(w).Encode(p)
		case http.MethodDelete:
			delete(profiles, name)
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})

	return httptest.NewServer(mux)
}

func TestAccExecutionProfileResource(t *testing.T) {
	server := newFakePortageServer(t)
	defer server.Close()

	providerConfig := fmt.Sprintf(`provider "portage" { api_url = %q }`, server.URL)

	resource.Test(t, resource.TestCase{
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + `
resource "portage_execution_profile" "test" {
  name          = "acc-test-exec"
  provider_type = "kubernetes"
  config        = jsonencode({ namespace = "default" })
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("portage_execution_profile.test", "name", "acc-test-exec"),
					resource.TestCheckResourceAttr("portage_execution_profile.test", "provider_type", "kubernetes"),
				),
			},
			{
				Config: providerConfig + `
resource "portage_execution_profile" "test" {
  name          = "acc-test-exec"
  provider_type = "databricks"
  config        = jsonencode({ namespace = "default" })
}
`,
				Check: resource.TestCheckResourceAttr("portage_execution_profile.test", "provider_type", "databricks"),
			},
		},
	})
}
