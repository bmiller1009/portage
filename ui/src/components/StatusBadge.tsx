const TERMINAL_OK = new Set(["SUCCEEDED"]);
const TERMINAL_BAD = new Set(["FAILED", "LOST"]);
const NEUTRAL = new Set(["CANCELED", "CANCELING"]);

export default function StatusBadge({ state }: { state: string }) {
  let className = "badge badge-active";
  if (TERMINAL_OK.has(state)) className = "badge badge-success";
  else if (TERMINAL_BAD.has(state)) className = "badge badge-failure";
  else if (NEUTRAL.has(state)) className = "badge badge-neutral";

  return <span className={className}>{state}</span>;
}
