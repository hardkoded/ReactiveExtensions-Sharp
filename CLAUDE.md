# RxSharp

A .NET port of RxJS. Package/namespace: `RxSharp`. Repo: `Rx-Sharp` (directory on disk may still be named `rxjs-sharp`).

## Why this exists

Upstream Puppeteer (JS) uses rxjs internally for timeout/retry/cancellation combinators (`Locator` actions, network-idle detection, screencast throttling). The long-term goal is to make this port usable inside `puppeteer-sharp` for the same purpose. See "Puppeteer-essential surface" below for the priority list this implies.

## Workflow rules

- **TDD, always**: for every primitive/operator, write the failing NUnit test first, then implement until it passes.
- **One branch, one PR, per feature**: each operator group / primitive / milestone slice lands as its own branch + PR merged to `main`. No large multi-feature branches.
- **Honor the original RxJS test suite**: don't invent test cases when an upstream one already exists. Port from RxJS tag `7.8.2` (the last stable pre-rewrite release — HEAD of the rxjs repo is an unstable 8.0 alpha with a different architecture). Upstream repo: `/Users/dario/Code/reactivex/rxjs`. Spec files live at the repo root under `spec/operators/*-spec.ts` and `spec/observables/*-spec.ts` at that tag (not under `packages/rxjs/spec/` — that nested layout only exists on the HEAD monorepo rewrite). To browse the tag without disturbing whatever branch is currently checked out there, use a throwaway worktree:
  ```
  git -C /Users/dario/Code/reactivex/rxjs worktree add --detach /tmp/rxjs-782-ref 7.8.2
  # ... read files under /tmp/rxjs-782-ref/spec/ ...
  git -C /Users/dario/Code/reactivex/rxjs worktree remove /tmp/rxjs-782-ref
  ```
- **Puppeteer usage must show up in tests, not just a one-off playground check**: `test/RxSharp.Tests/PuppeteerScenarios/` holds hand-written cases (no upstream spec equivalent) modeled on real Puppeteer usage patterns — see list below. These run in normal CI, independent of any one-time validation against the real puppeteer-sharp codebase.
- **Keep this file current**: update it with learnings as milestones land — API decisions that changed, gotchas found while porting upstream tests, anything a future session would otherwise have to rediscover.
- **README sells it**: runnable example, RxJS→RxSharp comparison snippet, badges, roadmap/parity status — not a dry stub.

## Conventions

- TFMs: `netstandard2.0;net8.0;net10.0` for the main library; test project targets `net10.0` only.
- Nullable reference types **enabled** (`Nullable=enable`) — a deliberate divergence from puppeteer-sharp (which has them off); compile-time only, doesn't affect binary consumption.
- Classic (block-scoped) namespaces, StyleCop-flavored `.editorconfig` (copied from puppeteer-sharp): no `this.` qualification, expression-bodied members preferred, braces required, 4-space indent, LF endings.
- `TreatWarningsAsErrors=true` via root `Directory.Build.props`.
- XML-doc completeness (`CS1591`, `SA160x`) is suppressed for now — deferred to the M4 docs pass, not worth blocking TDD cycles on it while the API is still moving.
- Test framework: NUnit + NUnit.Analyzers + NUnit3TestAdapter + NSubstitute (matches puppeteer-sharp's test stack).
- Layout is flat (`src/RxSharp/`, `test/RxSharp.Tests/`) at repo root, not nested under `lib/` like puppeteer-sharp — no samples/demos/docfx yet to justify that nesting. Revisit if the project grows to match that scope.
- One file per operator under `src/RxSharp/Operators/` (mirrors rxjs's own layout; also satisfies StyleCop's one-type-per-file rule for free).

## Core design

- `Observable<T>` implements BCL `IObservable<T>` for free interop, but the actual reactive engine (`Subscription`, `Subscriber<T>`, the `Operate()` helper) is custom, layered on top — same shape as rxjs's own internals.
- rxjs's `lift()`/`hasLift` virtual-dispatch cascade is dropped for v1 — it exists mainly to support `ConnectableObservable`/Subject-overridden operator behavior, out of scope initially.
- `Subject<T>`/`ReplaySubject<T>` implement `IObservable<T>, IObserver<T>` directly (not by subclassing `Observable<T>`), each exposing `.AsObservable()`.
- `Unit` struct stands in for rxjs's `Observable<never>` cases (cancellation/timeout streams).
- No hand-written 9-arity `pipe()`. Primary idiom is extension-method chaining (`source.Map(...).Filter(...).Take(5)`).
- `IScheduler` seam introduced early (one `TaskPoolScheduler` impl) threaded through time-based operators, so a `VirtualTimeScheduler` can be added later without rewriting those operators.

## Puppeteer-essential surface (priority within the broader port)

Puppeteer funnels all its rxjs usage through one wrapper file re-exporting ~40 symbols. These get priority:

- Core: `Observable`, `ReplaySubject`
- Creation: `of`, `from`, `defer`, `fromEvent`, `timer`, `concat`, `merge`, `race`, `raceWith`, `zip`, `forkJoin`, `combineLatest`, `EMPTY`, `NEVER`, `identity`, `noop`
- Async bridge: `firstValueFrom`, `lastValueFrom`
- Operators: `map`, `filter`, `mergeMap`, `concatMap`, `switchMap`, `mergeScan`, `bufferCount`, `take`, `first`, `takeUntil`, `startWith`, `distinctUntilChanged`, `defaultIfEmpty`, `throwIfEmpty`, `ignoreElements`, `debounceTime`, `delay`, `delayWhen`, `retry`, `catchError`, `tap`
- Custom combinators to replicate in `RxSharp.Extras`: `fromEmitterEvent` (wraps a .NET `event`), `fromAbortSignal` → `FromCancellationToken`, `timeout`, `filterAsync`, `retryAndRaceWithSignalAndTimer` (`retry` + `raceWith(fromCancellationToken, timeout)` — the combinator behind Puppeteer's `Locator` actions)

Explicitly low priority for now: `Subject`/`BehaviorSubject`/`AsyncSubject` beyond `ReplaySubject`, schedulers as a full hierarchy, `share`/`shareReplay`, `debounce`/`audit`/`sample`/`throttle` families, `groupBy`, `window*` operators, marble/testing utilities.

## Milestones

See the original plan for full detail. Summary: M0 scaffolding (done) → M1 core engine + first operator slice (done) → M2 broad operator/creation-function expansion + `RxSharp.Extras` + `PuppeteerScenarios` tests (first slice done: scheduler seam, `Timer`, `Race`/`RaceWith`, `TakeUntil`, `Retry`, async bridge, and the full `RetryAndRaceWithSignalAndTimer` combinator with a passing end-to-end retry+cancel+timeout test — still outstanding: `FromEmitterEvent`, `FilterAsync`, and the remaining transformation/filtering/combination operators) → M3 playground validation against a throwaway branch of puppeteer-sharp → M4 long tail (schedulers, marble testing, docs/CI polish).

No GitHub remote yet by explicit choice ("let's work local now") — milestones land as local feature branches merged into `main` with `--no-ff`, not real PRs, until that changes.

## Learnings

- **Operators must catch their own user-callback exceptions, not `Subscriber`.** Ported rxjs's `tap`-spec tests ("should raise error if next/error/complete handler raises error") revealed that a generic try/catch-and-forward inside `Subscriber<T>.OnNext`/`OnError`/`OnCompleted` double-swallows exceptions across nested operator chains (the innermost `Observable<T>.Subscribe`'s own try/catch intercepts the rethrow before it reaches the outer operator, and the target subscriber is already stopped by then, so the forward becomes a no-op). The correct, rxjs-faithful pattern: `Subscriber<T>` does plain guarded dispatch with no exception handling of its own; each operator (`Map`, `Filter`, `Tap`, `CatchError`, ...) wraps its own call into user-supplied delegates (project functions, predicates, side effects) in a local try/catch and explicitly calls `subscriber.OnError(ex)` on failure. Follow this pattern for every future operator.
- **Unhandled errors (no error handler anywhere in the chain) must not throw synchronously.** For the same structural reason above — nested `Subscribe` try/catches would intercept and swallow a synchronous rethrow before it reaches the caller. `RxConfig.OnUnhandledError` (default: throws on the thread pool) mirrors rxjs's `config.onUnhandledError`, which exists for exactly this reason, not just as a nice-to-have.
- **`Subscriber<T>.Create(...)` cannot be a static method on `Subscriber<T>` itself** — triggers CA1000 (no static members on generic types). Factory lives on a non-generic sibling `Subscriber` class, same pattern as `Observable`/`Observable<T>`.
- **StyleCop.Analyzers 1.1.118 mishandles primary-constructor base-list colons**: `private sealed class Foo(int x) : IBar` triggers a spacing conflict where SA1009 ("closing paren should not be followed by space") and SA1024 ("colon should be preceded by space") fight over the same `) :` token pair, and `dotnet format` cannot converge — it even left literal `<<<<<<<`/`>>>>>>>` conflict markers in files during a multi-TFM format pass. Fix: don't use primary-constructor syntax on classes that also declare a base/interface list; use a classic constructor + fields instead.
- **Target-typed `new()`/`new ()` has the same oscillation problem** (SA1000 "keyword must be followed by space" vs. the whitespace formatter's own preferred style never agreeing under `--verify-no-changes`). Avoid target-typed `new()` — always spell out the type (`new Observable<T>(...)`, `new object()`), which sidesteps the conflict entirely and reads clearer anyway.
- **`AnalysisLevel=latest-recommended` + `TreatWarningsAsErrors` is too strict for a fast-moving greenfield library.** Full NetAnalyzers "recommended" severity brings in CA1032 (standard exception ctor overloads), CA1063/CA1816 (full `IDisposable(bool)` + finalizer + `GC.SuppressFinalize` ceremony), CA1062 (null-check every public parameter), CA1716 (rename params that collide with keywords in other CLR languages, e.g. `error`), CA2000 (flags intentional-ownership-transfer disposable patterns as leaks) — all as build errors from day one. These are suppressed project-wide (see both `.csproj` `NoWarn` lists) until the M4 polish pass; don't remove the suppressions piecemeal without re-litigating the whole list, since they were added together for this exact reason.
- **`Subject<T>` needs the same delegate-based `Subscribe(onNext, onError, onComplete)` convenience overload as `Observable<T>`** — without it, callers can't pass a bare method group/lambda directly to a `Subject<T>`/`ReplaySubject<T>` instance (only to `.AsObservable()`), which real rxjs subjects support directly.
- **`ReplaySubject<T>.OnNext` must check the inherited stopped/disposed state *before* buffering**, not just before forwarding — otherwise a value nexted after `OnComplete`/`OnError` gets buffered (and later wrongly replayed to new subscribers) even though it's correctly *not* forwarded to current subscribers. Required adding a `protected bool IsStopped` accessor on `Subject<T>` for subclasses.
- **`SingleAssignmentDisposable` is used as a "reassign, don't dispose-the-old-one" cell**, not the throw-on-second-set type Rx.NET has under the same name. This is only safe because every place it's reassigned (`Take`, `First`, `CatchError`, `Retry`) reassigns *after* the previous disposable has already self-torn-down via the normal `Subscriber.OnError`/`OnCompleted` → `Unsubscribe` finally-block path. If a future operator needs to swap to a genuinely-still-live disposable, this type is the wrong tool — reach for an actual serial-disposable-with-dispose-on-reassign instead.
- **C# has no bottom type**, so `Observable<Unit>` sources that only ever error (`FromCancellationToken`, `Timeout`) can't be passed directly to `RaceWith<T>(Observable<T>...)` the way rxjs passes `Observable<never>` anywhere thanks to structural typing. The fix used here: `.Map<Unit, T>(value => throw new InvalidOperationException("unreachable"))` — safe specifically because these sources never call `OnNext`, only `OnError`, so the projection genuinely never runs. Don't reuse this trick for a source that might actually emit.
- **Time-based tests must not assert synchronously right after `Subscribe()`** when the operator under test uses `TaskPoolSchederler` (real, async delays) — `Retry`'s delay, `Timer`, etc. all complete on a background thread. Every such test needs a `ManualResetEventSlim` (or similar) that the `onComplete`/`onError` callback signals, with the assertions running after `signal.Wait(timeout)`. Got bitten by this once already (a "should retry until success" test that asserted 0ms after subscribing, before any retry had fired) — don't repeat it.
- **`IScheduler.Schedule` cancellation on `TaskPoolScheduler` relies on the `CancellationTokenSource` passed into `Task.Delay`** being cancelled *before* the delay elapses; `Race`'s loser-unsubscription path exercises this directly (a losing branch's `Timer` must never fire its callback after the race is won) and is covered by a test asserting the loser's side effect never runs.
