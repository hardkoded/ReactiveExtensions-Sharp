# Blog post material: ReactiveExtensionsSharp

Raw facts and content for a blog post about this project. No voice/tone direction here on purpose — just what
was built, why, and what's technically interesting about it. The writer should shape this into whatever the
actual post needs.

## The one-sentence pitch

ReactiveExtensionsSharp is a .NET port of RxJS — not "another Rx library," a *faithful port of RxJS specifically*: same operator
names, same semantics, same edge-case behavior, verified test-for-test against RxJS's own upstream spec suite
(tag `7.8.2`). If you know `source$.pipe(map(x => x * 2), filter(x => x > 0))`, you already know how to use it.

## The gap this fills in .NET

.NET already has a mature, first-class reactive library: [Rx.NET](https://github.com/dotnet/reactive)
(`System.Reactive`), which predates RxJS and in some ways inspired it. So why does a "port of RxJS" need to
exist separately?

Because Rx.NET and RxJS are not the same library with a different syntax — they're two independent
implementations of the Reactive Extensions *idea* that have diverged over a decade of separate evolution:
different operator names in places, different default behaviors for the same-named operator in others (e.g.
subject/replay semantics, error-swallowing edge cases), different creation-function surfaces, different testing
idioms. A .NET developer who already knows Rx.NET can pick up RxJS concepts fine, but *porting actual, working
RxJS code* — not just the idea of it, the literal pipelines — from a JS codebase to a C# one means translating
through those differences by hand, operator by operator, hoping the edge cases line up. There was no library
that let you take a `pipe(retry({delay}), raceWith(fromAbortSignal(...), timeout(...)))` line from a real JS
codebase and get the identical behavior in C# with a mechanical, not creative, translation.

The concrete motivating case: [Puppeteer](https://pptr.dev/) (the JS browser-automation library) uses rxjs
internally for exactly this kind of thing — retry-with-timeout-with-cancellation combinators behind its
`Locator` API, network-idle detection, screencast frame throttling. Its .NET port, puppeteer-sharp, doesn't use
Rx.NET for any of this — it hand-rolls the equivalent logic with `CancellationTokenSource`, `while(true)` retry
loops, and manual `catch` clauses to disambiguate "timed out" from "cancelled" from "just retry this attempt."
That code works, but it's bespoke, and every time upstream Puppeteer's JS reactive pipeline changes, someone has
to re-derive the C# equivalent from scratch by reading rxjs semantics and re-implementing them imperatively.
ReactiveExtensionsSharp exists so that translation becomes mechanical: the same rxjs operator, same name, same behavior, already
sitting there in C#.

### Concrete before/after: what this replaces

Puppeteer-sharp's real, current `Locator.RunWithRetryAsync` (retries a DOM action, racing a timeout):

```csharp
private async Task<IJSHandle> RunWithRetryAsync(
    Func<CancellationToken, Task<IJSHandle>> operation,
    CancellationToken cancellationToken)
{
    using var timeoutCts = Timeout > 0
        ? new CancellationTokenSource(Timeout)
        : new CancellationTokenSource();
    using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(timeoutCts.Token, cancellationToken);
    var linkedToken = linkedCts.Token;

    while (true)
    {
        try
        {
            return await operation(linkedToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
        {
            throw new TimeoutException($"Timed out after waiting {Timeout}ms");
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception) when (cancellationToken.IsCancellationRequested)
        {
            throw new OperationCanceledException(cancellationToken);
        }
        catch (Exception) when (timeoutCts.IsCancellationRequested)
        {
            throw new TimeoutException($"Timed out after waiting {Timeout}ms");
        }
        catch
        {
            try
            {
                await Task.Delay(RetryDelay, linkedToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
            {
                throw new TimeoutException($"Timed out after waiting {Timeout}ms");
            }
        }
    }
}
```

The ReactiveExtensionsSharp equivalent, proven against a real launched Chrome:

```csharp
await Observable.Defer(() => Observable.From(ClickOnceAsync()))
    .RetryAndRaceWithSignalAndTimer(timeout, causeFactory: null, retryDelay: TimeSpan.FromMilliseconds(50), cancellationToken)
    .FirstValueFrom();
```

Same behavior (retry until success, or fail with a distinguishable timeout vs. cancellation outcome), one
declarative line instead of a hand-rolled state machine with five catch clauses. This is the single clearest
"why" for the whole project.

## What was actually built (facts, for scale/credibility)

- 100+ operators and creation functions — effectively the entire RxJS 7.8.2 operator surface, not a curated
  subset. Includes the obvious ones (`map`, `filter`, `mergeMap`, `switchMap`, `debounceTime`, `retry`,
  `catchError`) and the much less commonly reimplemented ones: `groupBy`, `expand` (recursive re-projection),
  `bindCallback`/`bindNodeCallback` (turn a legacy callback-style API into a stream), `partition`, `using`
  (resource-scoped observables with automatic disposal), `onErrorResumeNext`, the entire `window*`/`buffer*`
  families, `share`/`shareReplay` with the full config surface (`resetOnError`/`resetOnComplete`/
  `resetOnRefCountZero`/custom multicast target).
- 800+ tests. Every one ported test-for-test from RxJS's own upstream spec suite (not reinvented from
  documentation or a changelog) — literally the same test cases RxJS itself uses to verify its own operators,
  translated to NUnit.
- A `VirtualTimeScheduler` + marble-style `TestScheduler` — the same testing idiom RxJS itself popularized:
  write a test that asserts a debounce/throttle/delay/retry-with-backoff pipeline behaves correctly across
  *virtual* time, with zero real wall-clock waiting. This is common practice in the JS reactive-programming
  world and much rarer in .NET, where time-based logic is more often tested with real `Task.Delay` + a
  stopwatch, or skipped in tests entirely.
- Validated end-to-end against a real, launched, headless Chrome browser (via puppeteer-sharp's own browser
  launcher) — not just synthetic unit tests. The retry-with-timeout-with-cancellation combinator above was
  proven to correctly (a) succeed once a delayed DOM element appears, and (b) time out correctly when it never
  does, against a real page.
- Published to NuGet using [Trusted Publishing](https://learn.microsoft.com/en-us/nuget/nuget-org/trusted-publishing) —
  a newer, OIDC-based publishing flow where GitHub Actions exchanges a short-lived token for a temporary
  (1-hour) NuGet API key at publish time, so no long-lived API key is ever stored as a secret anywhere.

## Technical/architectural highlights (the "war stories")

**A real, subtle bug class, found independently multiple times.** RxJS's own internal architecture has a
specific mechanism (`destination.add(this)` at subscriber-construction time) that makes downstream unsubscription
correctly cascade all the way up a chain of operators, even for a fully-synchronous source mid-loop. Porting
that correctly turned out to be genuinely tricky: an early, natural-looking implementation (assign a disposable
to a slot *after* calling `Subscribe()`, then dispose that slot later) works for asynchronous sources but
silently fails for synchronous ones, because the entire synchronous emission loop runs to completion *before*
`Subscribe()` even returns — so the "stop early" signal never has anywhere to attach in time. This exact bug was
independently rediscovered five separate times across the project's development (by different people/agents
working on different operators, unaware of each other's findings) before it was root-caused and fixed at the
architecture level with a shared helper, rather than patched operator-by-operator. It's a good, concrete example
of how something that sounds like a simple "just wire up cancellation" task can hide a real correctness bug that
only shows up in one specific, easy-to-miss scenario (synchronous sources), and how independent rediscovery of
the same bug is itself a signal that it's a structural issue, not a one-off mistake.

**A systematic test-coverage audit found real bugs, not just missing tests.** Late in the project, an audit
compared every ported test file against its real upstream RxJS spec file, operator by operator, to find gaps.
The expected outcome was "some operators have thinner test coverage than others." The actual outcome also
included four genuine, previously-undetected correctness bugs, found *while writing* the tests the audit
recommended — not from the audit itself, but from the process of actually exercising each operator's stated
edge cases:
- Two RxJS creation functions (`concat`, `merge`) had the exact synchronous-disposal bug described above, in a
  place nobody had checked yet, because they live in a different part of the codebase (creation functions, not
  operators) than where the bug had already been fixed.
- `combineLatest` completed too early in one specific edge case (a source that finishes without ever emitting a
  value) — confirmed wrong by reading RxJS's actual production source directly, not by assumption.
- `concatMap` could start its next queued value's inner observable *before* the previous one's cleanup/teardown
  logic had actually finished running, if that cleanup was expected to happen synchronously — a subtle ordering
  bug in exactly the kind of "finalize resource X before starting resource Y" guarantee that real-world code
  (closing a file handle, releasing a lock) depends on.
- `takeUntil`'s "second observable" (the one that signals "stop") had no error handler wired up at all, so if
  that second observable ever errored, the whole test process would crash outright instead of the error being
  handled the way RxJS's own implementation handles it (silently, deliberately, matching a documented but
  previously-**un-enforced** claim already sitting in the code's own comments).

**Built by parallelizing work across many independent AI coding sessions, each isolated in its own git
worktree.** Multiple full milestones of operator ports, bug fixes, and test-coverage work were done by running
several agent sessions concurrently, each on its own git branch/worktree, then reviewing and merging each
independently — rather than one long, serial coding session. This is a genuinely different way of building a
piece of open-source infrastructure than the traditional single-author-or-small-team model, and this project
(rapid, systematic, test-driven, spec-verified porting of a large, well-defined API surface) turned out to be a
particularly good fit for it, precisely *because* "port this one operator, verified against this one upstream
test file" is such a self-contained, independently-checkable unit of work.

## Things a .NET developer might not expect to see in a reactive library

- **Marble/virtual-time testing** as a first-class, built-in testing utility (`TestScheduler`, `Recorded<T>`,
  cold/hot observable helpers) — lets you write `scheduler.Record(source.DebounceTime(...))` and assert on exact
  emission timing without a single real millisecond of wall-clock wait in the test run.
- **`Expand`** — recursively re-project each emitted value back through itself (e.g. paginating an API by
  following a "next page" link found in each response) as a first-class stream operator, rather than a manual
  recursive async function.
- **`GroupBy`** as a stream operator — partition one observable into a dynamic set of per-key observable groups
  (each itself a full observable, emitting only that key's values, closable independently), not just a one-shot
  in-memory `IEnumerable.GroupBy`.
- **`BindCallback`/`BindNodeCallback`** — wrap an old-style, callback-based API (`Action<Action<T>>`, or Node's
  `(err, result) => ...` convention) into an observable that caches its result and replays it to every later
  subscriber, invoking the underlying callback-style function exactly once no matter how many times you
  subscribe.
- **`Share`/`ShareReplay` with a full reset-behavior config surface** — control independently whether an error,
  a completion, or the subscriber count dropping to zero should reset/tear down the shared underlying
  subscription versus keep it alive for a later resubscriber, plus the ability to plug in a custom multicast
  target (e.g. a `BehaviorSubject` instead of the default plain `Subject`).
- **`Using`** — allocate a resource and the observable that depends on it together, at subscribe time, with the
  resource's disposal automatically tied to the subscription's own lifetime (completion, error, or explicit
  unsubscribe) — a reactive-stream analogue of a `using` block, but scoped to a subscription's lifetime rather
  than a synchronous method's.
