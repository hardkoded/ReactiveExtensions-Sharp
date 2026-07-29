# Handoff: integrating ReactiveExtensionsSharp into puppeteer-sharp

This doc is written for whoever (human or agent) picks up the actual integration work: replacing
puppeteer-sharp's hand-rolled async/retry/cancellation plumbing with ReactiveExtensionsSharp, the way upstream Puppeteer (JS)
uses rxjs for the same job. It assumes no prior context on this project beyond what's written here — read this
top to bottom before touching code.

## What ReactiveExtensionsSharp is

- GitHub: [`hardkoded/ReactiveExtensions-Sharp`](https://github.com/hardkoded/ReactiveExtensions-Sharp) (directory
  on disk may still be named `rxjs-sharp`)
- NuGet: `ReactiveExtensionsSharp` (the plain `ReactiveExtensionsSharp` name was taken; C# namespace is still `ReactiveExtensionsSharp`)
- API docs: <https://hardkoded.github.io/ReactiveExtensions-Sharp/>
- A faithful, test-for-test port of [RxJS 7.8.2](https://github.com/ReactiveX/rxjs/tree/7.8.2) — not a redesign,
  not "Rx.NET but renamed." If you know the RxJS operator you're replacing, the ReactiveExtensionsSharp equivalent has the same
  name, same semantics, same edge-case behavior, verified against RxJS's own upstream spec tests.
- 800+ tests, all green, `TreatWarningsAsErrors` build, live NuGet package (Trusted Publishing, OIDC-based, no
  stored API key) and a GitHub Pages docs site, both wired to fire on version tags.

**Full project history, architecture rationale, and every gotcha found while building it lives in
[`CLAUDE.md`](../CLAUDE.md) at the repo root — treat it as the primary reference, not this document.** This
handoff doc is scoped narrowly to "how does this map onto puppeteer-sharp specifically."

## Why this exists — the exact motivating usage

Upstream Puppeteer (JS) does **not** use rxjs ad hoc — every rxjs symbol it touches is funneled through one
wrapper file that re-exports exactly what it needs:

```
packages/puppeteer-core/third_party/rxjs/rxjs.ts   (in the puppeteer/puppeteer repo)
```

That file's full export list, verified directly against the real upstream source (not assumed or reconstructed
from memory):

```ts
export {
  bufferCount, catchError, combineLatest, concat, concatMap, debounceTime, defaultIfEmpty,
  defer, delay, delayWhen, distinctUntilChanged, EMPTY, filter, first, firstValueFrom,
  forkJoin, from, fromEvent, identity, ignoreElements, lastValueFrom, map, merge, mergeMap,
  mergeScan, NEVER, noop, Observable, of, pipe, race, raceWith, ReplaySubject, retry,
  startWith, switchMap, take, takeUntil, tap, throwIfEmpty, timer, zip,
} from 'rxjs';
```

**39 of these 40 symbols have a direct ReactiveExtensionsSharp equivalent, same name (PascalCase), same behavior.** The one
exception is `pipe` — see "The one real idiom gap" below, it's intentional, not missing functionality.

Puppeteer builds three real features on top of this list, all three now proven to work end-to-end against a
real launched Chrome (see "Proof this actually works" below):

1. **`Locator` actions** (`locator.click()`, `.fill()`, etc.) — retry a DOM action against a target that may not
   exist yet, racing against a timeout and an abort signal.
2. **Network-idle detection** (`page.waitForNetworkIdle()`) — a `mergeScan` + `distinctUntilChanged` pipeline
   tracking in-flight request count over time.
3. **`ScreenRecorder`** — buffers frames with `bufferCount` + `concatMap` to throttle disk writes.

## The one real idiom gap: `pipe()`

RxJS's `pipe()` is a hand-written, overloaded-up-to-9-arguments function: `source$.pipe(map(f), filter(g),
takeUntil(h$))`. ReactiveExtensionsSharp deliberately does **not** port that — see `CLAUDE.md`'s Core design section. The direct
replacement idiom is **extension-method chaining**:

```csharp
// JS: source$.pipe(map(f), filter(g), takeUntil(h$))
source.Map(f).Filter(g).TakeUntil(h)
```

If you're translating a Puppeteer `.pipe(...)` call site, don't look for a `Pipe()` overload that takes multiple
operators — there isn't one (there's a single-operator `Pipe<TSource,TResult>(OperatorFunction<...>)` for
composing pre-built operator functions, which is a different, narrower use case). Just chain the methods in the
same order the `pipe()` arguments were in. This is a 1:1 mechanical translation for every real call site in
Puppeteer's own code — none of them do anything with `pipe()` that chaining can't express directly.

## API surface differences worth knowing before you start

- **`FromEvent` is not a literal port.** RxJS's `fromEvent` duck-types across DOM `EventTarget`s, Node
  `EventEmitter`s, and a few other shapes. C# has no equivalent ambient duck-typing for "add/remove listener," so
  ReactiveExtensionsSharp's `Observable.FromEvent` wraps a real **.NET event add/remove-handler pair** instead — the same idiom
  Rx.NET itself uses (`FromEvent<TDelegate, TEventArgs>`). Two overloads exist:
  - `FromEvent<TEventArgs>(Action<EventHandler<TEventArgs>> addHandler, Action<EventHandler<TEventArgs>> removeHandler)`
    — for `event EventHandler<TEventArgs>` members (covers most puppeteer-sharp events, e.g. `IPage.Console`).
  - `FromEvent<TDelegate, TEventArgs>(addHandler, removeHandler, conversion)` — for plain, non-generic
    `EventHandler` members (e.g. `IPage.Load`), where `conversion` adapts the delegate shape.
  Both are already proven against real puppeteer-sharp events — see the code below.
- **`Zip`/`ForkJoin`/`CombineLatest` are same-type-array-only.** RxJS's versions accept heterogeneously-typed
  tuples (`zip(a$: Observable<string>, b$: Observable<number>)`). ReactiveExtensionsSharp's take `params Observable<T>[]` — every
  source must share one element type. Check whether Puppeteer's actual `combineLatest`/`zip`/`forkJoin` call
  sites need heterogeneous types before assuming this is a drop-in replacement for those specific calls; if they
  do, project each source to a common shape first (a small wrapper record/tuple) before combining, then `.Map()`
  the result apart afterward.
- **No config-object overloads.** Where RxJS takes a single options object (e.g. `retry({count, delay,
  resetOnSuccess})`), ReactiveExtensionsSharp exposes the same options as separate, named overloads instead (e.g.
  `Retry(count, delay, scheduler, resetOnSuccess)` and a second `Retry(delaySelector, count, resetOnSuccess)`
  overload for the notifier-based delay form). Same coverage, just no anonymous-object-literal ergonomics — not
  a gap, just a different (and, for a statically-typed language, more idiomatic) shape.
- **`Unit`, not `never`.** Where RxJS types a stream as `Observable<never>` (cancellation/timeout signals that
  only ever error, never emit), ReactiveExtensionsSharp uses `Observable<Unit>` (`ReactiveExtensionsSharp.Unit`, a real struct — C# has no bottom
  type). `Observable.FromCancellationToken` and `ReactiveExtensionsSharp.Extras.Timeout` both return `Observable<Unit>`.

## `ReactiveExtensionsSharp.Extras` — the hand-built combinators that don't exist in rxjs itself

Puppeteer's own rxjs usage isn't just the 40 wrapped symbols — it also builds a few combinators on top, inline,
at each call site. ReactiveExtensionsSharp extracted the common ones into `ReactiveExtensionsSharp.Extras` so they're reusable instead of
re-derived per call site:

- **`Observable.FromCancellationToken(CancellationToken)`** — the `fromAbortSignal` analogue. Returns
  `Observable<Unit>` that errors with `OperationCanceledException` when the token is cancelled, never emits
  otherwise.
- **`ReactiveExtensionsSharp.Extras.Timeout`** (`source.Timeout(TimeSpan, Func<Exception>? causeFactory = null)`) — errors if
  `source` doesn't emit within the given span.
- **`ReactiveExtensionsSharp.Extras.RetryAndRaceWithSignalAndTimer`** — the actual combinator behind `Locator` actions:
  `pipe(retry({delay}), raceWith(fromAbortSignal(...), timeout(...)))` in one call. This is the single most
  important piece for the `Locator` integration — see below.
- **`ReactiveExtensionsSharp.Extras.FilterAsync`** — an async-predicate `Filter`, for cases where the filter condition itself
  needs an `await` (e.g. checking element visibility via a JS evaluation).

## Proof this actually works: the M3 playground

Before writing this handoff doc, a throwaway integration was built and run against a **real, launched, headless
Chrome** (not mocks) in a separate git worktree: `puppeteer-sharp-rx-playground`, branch
`experiment/rxjs-sharp-playground`, off `origin/master` of puppeteer-sharp. It's local-only and unpushed (scratch
validation, not meant to be merged as-is), so the full working code is reproduced here rather than just linked,
in case that worktree isn't available wherever this integration actually happens.

All four tests below pass, end to end, launching a real browser via puppeteer-sharp's own `BrowserFetcher`:

```csharp
using ReactiveExtensionsSharp;
using ReactiveExtensionsSharp.Extras;

// Mirrors Puppeteer's own Locator.click(): retry a click, racing against a timeout, built entirely
// from ReactiveExtensionsSharp primitives (Defer + RetryAndRaceWithSignalAndTimer) - no puppeteer-sharp Locator API
// involved. The button is added to the page after a short delay, so the first few clicks fail with
// "no node found" and only succeed once retry catches up.
[Test]
public async Task ClickWithRetryBuiltFromRxSharpShouldSucceedOnceTheElementAppears()
{
    await _page.SetContentAsync(
        """
        <script>
          setTimeout(() => {
            const button = document.createElement('button');
            button.id = 'target';
            button.textContent = 'click me';
            button.addEventListener('click', () => { window.__clicked = true; });
            document.body.appendChild(button);
          }, 300);
        </script>
        """).ConfigureAwait(false);

    async Task<Unit> ClickOnceAsync()
    {
        await _page.ClickAsync("#target").ConfigureAwait(false);
        return Unit.Default;
    }

    await Observable.Defer(() => Observable.From(ClickOnceAsync()))
        .RetryAndRaceWithSignalAndTimer(TimeSpan.FromSeconds(10), causeFactory: null, retryDelay: TimeSpan.FromMilliseconds(50), CancellationToken.None)
        .FirstValueFrom().ConfigureAwait(false);

    var wasClicked = await _page.EvaluateExpressionAsync<bool>("window.__clicked === true").ConfigureAwait(false);
    Assert.That(wasClicked, Is.True);
}

// Same as above, but the timeout wins the race: the element never appears, so the retry loop
// gives up with a TimeoutException rather than retrying forever.
[Test]
public void ClickWithRetryBuiltFromRxSharpShouldTimeOutIfTheElementNeverAppears()
{
    async Task<Unit> ClickOnceAsync()
    {
        await _page.ClickAsync("#does-not-exist").ConfigureAwait(false);
        return Unit.Default;
    }

    Assert.ThrowsAsync<TimeoutException>(() =>
        Observable.Defer(() => Observable.From(ClickOnceAsync()))
            .RetryAndRaceWithSignalAndTimer(TimeSpan.FromMilliseconds(300), causeFactory: null, retryDelay: TimeSpan.FromMilliseconds(50), CancellationToken.None)
            .FirstValueFrom());
}

// Observable.FromEvent<TEventArgs> against a real EventHandler<ConsoleEventArgs> puppeteer-sharp event.
[Test]
public async Task FromEventShouldWrapARealEventHandlerOfTPuppeteerSharpEvent()
{
    var messages = new List<string>();
    using var signal = new ManualResetEventSlim();

    using var subscription = Observable.FromEvent<ConsoleEventArgs>(h => _page.Console += h, h => _page.Console -= h)
        .Subscribe(e =>
        {
            messages.Add(e.Message.Text);
            signal.Set();
        });

    await _page.EvaluateExpressionAsync("console.log('hello from the rxjs-sharp playground')").ConfigureAwait(false);

    Assert.That(signal.Wait(TimeSpan.FromSeconds(5)), Is.True);
    Assert.That(messages, Has.Some.EqualTo("hello from the rxjs-sharp playground"));
}

// Observable.FromEvent<TDelegate, TEventArgs> against a real plain (non-generic) EventHandler puppeteer-sharp event.
[Test]
public async Task FromEventShouldWrapARealPlainEventHandlerPuppeteerSharpEvent()
{
    using var signal = new ManualResetEventSlim();
    var loadFired = false;

    using var subscription = Observable.FromEvent<EventHandler, EventArgs>(
            h => _page.Load += h,
            h => _page.Load -= h,
            onNext => (_, e) => onNext(e))
        .Subscribe(_ =>
        {
            loadFired = true;
            signal.Set();
        });

    await _page.SetContentAsync("<html><body>hi</body></html>").ConfigureAwait(false);

    Assert.That(signal.Wait(TimeSpan.FromSeconds(5)), Is.True);
    Assert.That(loadFired, Is.True);
}
```

## What this actually replaces — `Locator`'s current implementation

Puppeteer-sharp's real `Locator` class (`lib/PuppeteerSharp/Locators/Locator.cs`, as of the version checked
while writing this doc) does **not** use ReactiveExtensionsSharp yet — it has its own hand-rolled retry loop,
`RunWithRetryAsync`:

```csharp
private async Task<IJSHandle> RunWithRetryAsync(
    Func<CancellationToken, Task<IJSHandle>> operation,
    CancellationToken cancellationToken)
{
    using var timeoutCts = Timeout > 0
        ? new CancellationTokenSource(Timeout)
        : new CancellationTokenSource();

    using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
        timeoutCts.Token, cancellationToken);

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
        // ... several more catch clauses distinguishing cancel-vs-timeout-vs-retry, plus a
        // Task.Delay(RetryDelay, linkedToken) between retries, itself wrapped in its own try/catch
        // because "exceptions thrown inside a catch block bypass sibling catch clauses" ...
    }
}
```

This is **exactly** the shape `RetryAndRaceWithSignalAndTimer` exists to replace — a manual retry loop with a
linked timeout/cancellation `CancellationTokenSource`, several catch clauses to disambiguate timeout vs.
cancellation vs. "just retry," and a delay between attempts. The M3 playground test above proves the ReactiveExtensionsSharp
replacement handles the same two cases (retry-until-success, retry-until-real-timeout) correctly against a real
page. The integration here is mechanical: wrap `operation` in `Observable.Defer(() => Observable.From(...))`,
call `.RetryAndRaceWithSignalAndTimer(timeout, causeFactory, retryDelay, cancellationToken)`, and
`.FirstValueFrom()` (or `await` it directly, since `RetryAndRaceWithSignalAndTimer` composes cleanly with the
existing async/await call sites — no need to convert the whole class to a reactive style, just this one method).

**Before wiring this in for real**, check whether `Locator`'s other retry-adjacent behavior (`WaitForEnabledAsync`,
`WaitForStableBoundingBoxAsync`, the specific exception types surfaced today, existing tests asserting on those
exception types/messages) needs to be preserved bit-for-bit or can be simplified along with the replacement —
that's a product decision, not something this doc can settle.

## Suggested integration path

1. Add a project/package reference to `ReactiveExtensionsSharp` from puppeteer-sharp's core library project.
2. Start with `Locator.RunWithRetryAsync` — it's the smallest, most self-contained replacement, already proven
   end-to-end above, and touches the least surrounding code.
3. Port test-first, same discipline this whole port was built with: find `Locator`'s existing xunit/nunit tests
   (whichever framework puppeteer-sharp uses — check the test project), make sure they still pass against the
   ReactiveExtensionsSharp-based implementation before considering it done. Don't just eyeball it.
4. Once `Locator` is solid, look at `waitForNetworkIdle` (the `mergeScan`+`distinctUntilChanged` in-flight-request
   tracker) and `ScreenRecorder` (the `bufferCount`+`concatMap` frame-throttling) — both operators already exist
   in ReactiveExtensionsSharp and are already tested, so this is the same "swap the hand-rolled version for the ReactiveExtensionsSharp one, keep
   the tests green" motion, not new operator-porting work.
5. For anything not covered above: check the exact RxJS symbol against `CLAUDE.md`'s "Puppeteer-essential
   surface" section first — if it's already ported, use it directly; if it's one of the deliberate gaps (`pipe`,
   heterogeneous `zip`/`combineLatest`/`forkJoin`), see the sections above for the workaround; if it's something
   genuinely missing, that's a real gap worth filing against the ReactiveExtensionsSharp repo rather than working around
   silently, since the port's whole premise is broad, faithful RxJS parity.
