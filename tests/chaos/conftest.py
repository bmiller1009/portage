"""Fixtures for live fault-injection tests against a real Kubernetes
cluster with the charts/portage Helm release installed and reachable via
kubectl (spec.md §57 — "a mandatory v1.0 acceptance test"). Not run in
CI — needs a live cluster and a real execution provider the reconciler
can actually submit work to, same category as
tests/integration/test_s3_provider_minio.py. Run manually, e.g. over SSH
on the project's remote dev box:

    PORTAGE_CHAOS_NAMESPACE=portage-system \
    MINIO_ENDPOINT=http://minio.portage-storage.svc.cluster.local:9000 \
    python -m pytest tests/chaos -v -s

See tests/chaos/README.md for what infrastructure this expects to
already exist (the Helm release, its RBAC, its credentials Secret).
"""

import contextlib
import json
import os
import subprocess
import time
import uuid
from pathlib import Path

import httpx
import pytest
import yaml

pytestmark = pytest.mark.skipif(
    "PORTAGE_CHAOS_NAMESPACE" not in os.environ,
    reason="requires a live cluster with the Portage Helm release installed (PORTAGE_CHAOS_NAMESPACE env var)",
)

NAMESPACE = os.environ.get("PORTAGE_CHAOS_NAMESPACE", "")
RELEASE = os.environ.get("PORTAGE_CHAOS_RELEASE", "portage")
FULLNAME = f"{RELEASE}-portage"


def unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def kubectl(*args: str) -> str:
    result = subprocess.run(
        ["kubectl", "-n", NAMESPACE, *args], capture_output=True, text=True, check=True
    )
    return result.stdout


def pod_names(component: str) -> list[str]:
    out = kubectl("get", "pods", "-l", f"app.kubernetes.io/component={component}", "-o", "name")
    return [line for line in out.splitlines() if line]


def delete_pod(pod_name: str) -> None:
    """A real, hard kill — not a graceful drain — matching spec §57's
    "control-plane destruction" language. --grace-period=0 --force sends
    SIGKILL immediately rather than waiting out a termination grace
    period, which would mask exactly the crash-mid-work case this suite
    exists to prove is safe."""
    kubectl("delete", pod_name, "--grace-period=0", "--force")


IN_CLUSTER_SERVER = "https://kubernetes.default.svc"
# RFC 5737 TEST-NET-1 — guaranteed non-routable, so a connection to it
# times out rather than being immediately refused, matching a real
# network partition more closely than "connection refused" would.
UNREACHABLE_SERVER = "https://192.0.2.1:6443"


def _outage_test_kubeconfig_manifest(configmap_name: str, server: str) -> str:
    return f"""apiVersion: v1
kind: ConfigMap
metadata:
  name: {configmap_name}
  namespace: {NAMESPACE}
data:
  kubeconfig: |
    apiVersion: v1
    kind: Config
    clusters:
      - name: in-cluster
        cluster:
          certificate-authority: /var/run/secrets/kubernetes.io/serviceaccount/ca.crt
          server: {server}
    contexts:
      - name: in-cluster
        context:
          cluster: in-cluster
          user: in-cluster
    current-context: in-cluster
    users:
      - name: in-cluster
        user:
          tokenFile: /var/run/secrets/kubernetes.io/serviceaccount/token
"""


def start_outage_test_reconciler(server: str, timeout: float = 90) -> tuple[str, str]:
    """Stands up a throwaway, unmanaged Pod — an exact copy of the real
    reconciler Deployment's current container spec (same image, same
    DATABASE_URL/credentials env) — with its kubeconfig fixed to `server`
    *before the pod is ever created*, mounted from a dedicated ConfigMap
    that's never modified afterward. Returns (pod_name, configmap_name)
    once Running, for cleanup via stop_outage_test_reconciler().

    Deliberately not a runtime ConfigMap swap against the real reconciler
    (an earlier version of this test did that): confirmed live that
    changing an already-mounted ConfigMap's content while a pod keeps
    running races with kubelet's propagation in a way that made the test
    genuinely non-deterministic — sometimes the reconciler's very next
    tick saw the update, sometimes it silently kept serving stale content
    for several ticks with no observable signal that it hadn't updated
    yet. Baking the value in before the pod exists at all removes that
    window entirely: kubelet mounts a ConfigMap's content as of pod
    creation, so there's nothing to race."""
    deploy = json.loads(kubectl("get", "deployment", f"{FULLNAME}-reconciler", "-o", "json"))
    container = deploy["spec"]["template"]["spec"]["containers"][0]

    suffix = uuid.uuid4().hex[:8]
    pod_name = f"outage-test-reconciler-{suffix}"
    configmap_name = f"outage-test-kubeconfig-{suffix}"

    subprocess.run(
        ["kubectl", "apply", "-f", "-"],
        input=_outage_test_kubeconfig_manifest(configmap_name, server),
        text=True,
        check=True,
    )

    pod_manifest = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": pod_name,
            "namespace": NAMESPACE,
            "labels": {"app.kubernetes.io/name": "portage", "app.kubernetes.io/component": "outage-test"},
        },
        "spec": {
            "restartPolicy": "Never",
            "containers": [
                {
                    "name": "reconciler",
                    "image": container["image"],
                    "command": container.get("command"),
                    "env": container.get("env", []),
                    "envFrom": container.get("envFrom", []),
                    "volumeMounts": [{"name": "kubeconfig", "mountPath": "/etc/portage", "readOnly": True}],
                }
            ],
            "volumes": [{"name": "kubeconfig", "configMap": {"name": configmap_name}}],
        },
    }
    subprocess.run(["kubectl", "apply", "-f", "-"], input=json.dumps(pod_manifest), text=True, check=True)

    deadline = time.time() + timeout
    phase = ""
    while time.time() < deadline:
        phase = kubectl("get", "pod", pod_name, "-o", "jsonpath={.status.phase}").strip()
        if phase == "Running":
            return pod_name, configmap_name
        time.sleep(2)
    raise TimeoutError(f"outage-test reconciler pod never became Running within {timeout}s (last phase={phase!r})")


def stop_outage_test_reconciler(pod_name: str, configmap_name: str) -> None:
    kubectl("delete", "pod", pod_name, "--grace-period=0", "--force")
    kubectl("delete", "configmap", configmap_name)


def wait_for_ready_replicas(component: str, expected: int, timeout: float = 120) -> None:
    # Kubernetes omits status.readyReplicas entirely (empty jsonpath
    # output, not "0") once it drops to zero, rather than reporting "0".
    expected_str = "" if expected == 0 else str(expected)
    deadline = time.time() + timeout
    while time.time() < deadline:
        out = kubectl(
            "get", "deployment", f"{FULLNAME}-{component}", "-o", "jsonpath={.status.readyReplicas}"
        )
        if out.strip() == expected_str:
            return
        time.sleep(2)
    raise TimeoutError(f"{component} deployment did not reach {expected} ready replicas within {timeout}s")


def wait_for_pod_count(component: str, expected: int, timeout: float = 120) -> None:
    """Waits until *exactly* `expected` pods matching `component` exist —
    not just until the Deployment's readyReplicas reaches that number.
    readyReplicas updates as soon as a terminating pod leaves the
    Service's ready endpoints, which can be well before the pod's
    process actually receives and finishes handling SIGTERM (the default
    terminationGracePeriodSeconds window); confirmed live twice — scaling
    the reconciler down and immediately proceeding let a stale,
    still-alive pod race to correctly process a run before the intended
    scenario (an unreachable-provider throwaway pod, or a kill-and-wait
    single-replica test) ever got a chance to. Callers that need "exactly
    N reconciler/api processes are actually running" (not just "the
    Deployment says N desired") should wait for this, not
    wait_for_ready_replicas alone."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if len(pod_names(component)) == expected:
            return
        time.sleep(2)
    raise TimeoutError(f"{component} did not settle at exactly {expected} pods within {timeout}s")


def _single_replica_fixture(component: str):
    """Scales a component to 1 replica for the duration of a test, then
    restores it to 2 (this Helm release's configured HA count) — always,
    even if the test fails. Used by tests that need a kill to be a real
    restart, not masked by the other HA replica picking up the slack
    (that's a separate scenario, test_ha_deployment.py)."""

    @pytest.fixture
    def _fixture():
        kubectl("scale", f"deployment/{FULLNAME}-{component}", "--replicas=1")
        wait_for_ready_replicas(component, 1)
        wait_for_pod_count(component, 1)
        try:
            yield
        finally:
            kubectl("scale", f"deployment/{FULLNAME}-{component}", "--replicas=2")
            wait_for_ready_replicas(component, 2)

    return _fixture


single_reconciler_replica = _single_replica_fixture("reconciler")
single_api_replica = _single_replica_fixture("api")


@contextlib.contextmanager
def port_forward_api(port: int, timeout: float = 30):
    """Port-forwards the real API Service on `port` until the context
    exits. A fresh call always gets a fresh tunnel process — confirmed
    live (test_control_plane_recovery.py's API-restart scenario) that an
    existing `kubectl port-forward svc/X` tunnel does not reliably
    survive a window where the Service has *zero* ready backends (which
    a single-replica kill-and-replace genuinely creates for a few
    seconds): the tunnel process itself can exit rather than just
    blocking until a new backend appears, so callers that need to talk
    to the API *after* a full-outage window like that should open a new
    one on a different port rather than trust a pre-existing session
    tunnel is still alive."""
    proc = subprocess.Popen(
        ["kubectl", "-n", NAMESPACE, "port-forward", f"svc/{FULLNAME}-api", f"{port}:8000"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    base_url = f"http://127.0.0.1:{port}"
    deadline = time.time() + timeout
    reachable = False
    while time.time() < deadline:
        try:
            httpx.get(f"{base_url}/health", timeout=1).raise_for_status()
            reachable = True
            break
        except httpx.HTTPError:
            time.sleep(1)
    if not reachable:
        proc.terminate()
        raise RuntimeError("API port-forward never became reachable")
    try:
        yield base_url
    finally:
        proc.terminate()
        proc.wait(timeout=10)


@pytest.fixture(scope="session")
def api_base_url():
    """The tests talk to the real REST surface a real operator/CLI would,
    not a direct DB session, since these tests exercise the live
    process, not just its logic."""
    with port_forward_api(18123) as base_url:
        yield base_url


@pytest.fixture(scope="session")
def api_client(api_base_url):
    with httpx.Client(base_url=api_base_url, timeout=30) as client:
        yield client


@pytest.fixture(scope="session")
def environment_name(api_client):
    """Seeds a real kubernetes+s3 Environment via the live API — pointed
    at the same MinIO and Spark Operator scripts/dev-up.sh already wires
    up (docs/providers/kubernetes.md, docs/providers/s3.md) — so runs
    submitted through it are real Spark jobs, not fakes. The
    ExecutionProfile's kubeconfig_path targets the in-cluster kubeconfig
    charts/portage's reconciler.inClusterKubeconfig mounts, since the
    reconciler runs inside the same cluster it submits to."""
    exec_name = unique("chaos-exec")
    resp = api_client.post(
        "/v1/execution-profiles",
        json={
            "name": exec_name,
            "provider": "kubernetes",
            "config": {
                "namespace": "default",
                "service_account": "spark",
                "image": "portage/wordcount:0.1.0",
                "kubeconfig_path": "/etc/portage/kubeconfig",
            },
        },
    )
    resp.raise_for_status()

    storage_name = unique("chaos-storage")
    resp = api_client.post(
        "/v1/storage-profiles",
        json={
            "name": storage_name,
            "provider": "s3",
            "config": {"endpoint_url": os.environ["MINIO_ENDPOINT"]},
            "credential_reference": {"provider": "env", "reference": "PORTAGE_CHAOS_MINIO"},
        },
    )
    resp.raise_for_status()

    env_name = unique("chaos-env")
    resp = api_client.post(
        "/v1/environments",
        json={
            "name": env_name,
            "execution_provider": "kubernetes",
            "execution_profile_name": exec_name,
            "storage_provider": "s3",
            "storage_profile_name": storage_name,
        },
    )
    resp.raise_for_status()

    for dataset_name, uri in [
        ("wordcount.raw", "s3a://portage-phase0/wordcount/input.txt"),
        ("wordcount.counts", f"s3a://portage-phase0/chaos/{env_name}/output"),
    ]:
        resp = api_client.post(
            "/v1/datasets",
            json={"dataset_name": dataset_name, "environment_name": env_name, "kind": "path", "uri": uri},
        )
        resp.raise_for_status()

    resp = api_client.post(
        "/v1/artifacts",
        json={
            "artifact_name": "wordcount",
            "artifact_version": "0.1.0",
            "environment_name": env_name,
            "kind": "path",
            # Unused by the Kubernetes provider for python-wheel workloads
            # (the wheel is already pip-installed into the image referenced
            # by the ExecutionProfile) — this binding only needs to exist
            # to satisfy artifact:// resolution (spec §51).
            "uri": "s3a://portage-phase0/wordcount/dist/wordcount-0.1.0-py3-none-any.whl",
        },
    )
    resp.raise_for_status()

    return env_name


EXAMPLES_DIR = Path(__file__).resolve().parents[2] / "examples"


@pytest.fixture(scope="session")
def workload_ref(api_client):
    """Registers the real wordcount.yaml fixture as a WorkloadDefinition
    via the live API — the same file the Kubernetes execution provider
    has been live-verified against since Phase 0. 409 (already
    registered, e.g. left over from an earlier suite run against the same
    long-lived cluster) is treated as success, not a failure — this
    fixture only needs the definition to exist, not to have created it."""
    with open(EXAMPLES_DIR / "wordcount.yaml") as f:
        definition = yaml.safe_load(f)

    resp = api_client.post("/v1/workloads", json=definition)
    if resp.status_code != 409:
        resp.raise_for_status()
    return definition["metadata"]["name"], definition["metadata"]["version"]


def wait_for_terminal_state(api_client, run_id: str, timeout: float = 300) -> str:
    from control_plane.run_state import TERMINAL_STATES

    terminal = {s.value for s in TERMINAL_STATES}
    deadline = time.time() + timeout
    last_state = None
    while time.time() < deadline:
        resp = api_client.get(f"/v1/runs/{run_id}")
        resp.raise_for_status()
        last_state = resp.json()["state"]
        if last_state in terminal:
            return last_state
        time.sleep(3)
    raise TimeoutError(f"run {run_id} did not reach a terminal state within {timeout}s (last: {last_state})")
