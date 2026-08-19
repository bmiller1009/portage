export default function ConformancePage() {
  return (
    <div>
      <h1>Conformance</h1>
      <p className="muted">
        This page is not yet implemented — but static and dynamic portability conformance testing
        itself is: <code>plane conformance test</code> and <code>plane conformance report</code>
        (spec §21) are real and live-verified, backed by <code>GET /v1/conformance/*</code>. Use
        the CLI until this page exists; this is an honest placeholder for the UI specifically, not
        for the underlying capability.
      </p>
    </div>
  );
}
