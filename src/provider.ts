/**
 * The shape of a third party we write to. Three things can happen to a write:
 *
 *   ok       the provider confirmed it
 *   refused  the provider definitively rejected it (validation, business rule);
 *            nothing changed on their side
 *   (throw)  we never saw an answer: timeout, connection reset, 5xx from a
 *            proxy. The write MAY have applied. This is the case most
 *            integrations get wrong by treating it as either success or failure.
 *
 * `requestId` is the operation's stable id. It is sent on every attempt so the
 * provider can deduplicate, and it is what `lookup` searches by during recovery.
 */
export type ProviderOutcome<R = unknown> = { ok: true; result: R } | { ok: false; refusal: { code: string; message: string } };

export interface Provider<P = unknown, R = unknown> {
  perform(kind: string, payload: P, requestId: string): Promise<ProviderOutcome<R>>;
  /** Did a previous attempt with this requestId apply? `null` means "no such write". */
  lookup(requestId: string): Promise<R | null>;
}

export class LostAnswerError extends Error {
  readonly code = "lost_answer";
  constructor(message = "no answer from provider") {
    super(message);
  }
}

/**
 * A scripted provider for tests. Each call to `perform` consumes the next
 * scripted behavior; `lost_after_apply` applies the write and then loses the
 * answer, which is the dangerous case.
 */
export type ScriptedBehavior = "ok" | "refuse" | "lost_before_apply" | "lost_after_apply";

export class ScriptedProvider implements Provider<Record<string, unknown>, { providerRef: string }> {
  readonly performCalls: Array<{ kind: string; requestId: string; behavior: ScriptedBehavior }> = [];
  readonly lookupCalls: string[] = [];
  private readonly applied = new Map<string, { providerRef: string }>();
  private script: ScriptedBehavior[];

  constructor(script: ScriptedBehavior[]) {
    this.script = [...script];
  }

  async perform(kind: string, _payload: Record<string, unknown>, requestId: string) {
    const behavior = this.script.shift() ?? "ok";
    this.performCalls.push({ kind, requestId, behavior });
    // A well-behaved provider dedupes on the caller's request id.
    const already = this.applied.get(requestId);
    if (already && behavior !== "refuse") return { ok: true as const, result: already };

    switch (behavior) {
      case "ok": {
        const result = { providerRef: `ref_${requestId.slice(0, 8)}` };
        this.applied.set(requestId, result);
        return { ok: true as const, result };
      }
      case "refuse":
        return { ok: false as const, refusal: { code: "slot_unavailable", message: "that time is not available" } };
      case "lost_before_apply":
        throw new LostAnswerError("connection reset before the request was sent");
      case "lost_after_apply":
        this.applied.set(requestId, { providerRef: `ref_${requestId.slice(0, 8)}` });
        throw new LostAnswerError("read timeout after the request was sent");
    }
  }

  async lookup(requestId: string) {
    this.lookupCalls.push(requestId);
    return this.applied.get(requestId) ?? null;
  }

  /** What the provider believes it has applied; the test oracle. */
  appliedWrites(): string[] {
    return [...this.applied.keys()];
  }
}
