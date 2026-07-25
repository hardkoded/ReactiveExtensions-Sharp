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

See the original plan for full detail. Summary: M0 scaffolding (done) → M1 core engine + first operator slice → M2 broad operator/creation-function expansion + `RxSharp.Extras` + `PuppeteerScenarios` tests → M3 playground validation against a throwaway branch of puppeteer-sharp → M4 long tail (schedulers, marble testing, docs/CI polish).

## Learnings

_(updated as milestones land)_
