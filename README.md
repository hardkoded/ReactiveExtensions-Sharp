# RxSharp

[![build](https://github.com/hardkoded/ReactiveExtensions-Sharp/actions/workflows/build.yml/badge.svg)](https://github.com/hardkoded/ReactiveExtensions-Sharp/actions/workflows/build.yml)
[![NuGet](https://img.shields.io/nuget/v/ReactiveExtensionsSharp.svg)](https://www.nuget.org/packages/ReactiveExtensionsSharp/)
[![docs](https://img.shields.io/badge/docs-API%20reference-blue)](https://hardkoded.github.io/ReactiveExtensions-Sharp/)
[![license](https://img.shields.io/github/license/hardkoded/ReactiveExtensions-Sharp)](LICENSE)

A .NET port of [RxJS](https://rxjs.dev/) — same operators, same semantics, same names you already know, in idiomatic C#.

Why a new Rx library when [Rx.NET](https://github.com/dotnet/reactive) exists? RxSharp isn't trying to replace it — it's a deliberately faithful port of *RxJS specifically*, built to make it painless to bring JS reactive code (and the libraries built on it, like [Puppeteer](https://pptr.dev/)) over to .NET without re-learning a different Rx dialect. If you know `pipe(map(...), filter(...), takeUntil(...))`, you already know RxSharp.

## Quick taste

```js
// RxJS
import { fromEvent, interval } from 'rxjs';
import { map, filter, takeUntil } from 'rxjs/operators';

const clicks$ = fromEvent(button, 'click');
interval(1000)
  .pipe(
    map(x => x * x),
    filter(x => x % 2 === 0),
    takeUntil(clicks$),
  )
  .subscribe(x => console.log(x));
```

The C# is close enough to read side by side — real, compiling code, not pseudocode:

<!-- snippet: quick-taste-csharp -->
<a id='snippet-quick-taste-csharp'></a>
```cs
var clicks = Observable.FromEvent<EventArgs>(h => button.Click += h, h => button.Click -= h);

Observable.Interval(TimeSpan.FromSeconds(1))
    .Map(x => x * x)
    .Filter(x => x % 2 == 0)
    .TakeUntil(clicks)
    .Subscribe(x => Console.WriteLine(x));
```
<sup><a href='https://github.com/hardkoded/ReactiveExtensions-Sharp/blob/main/test/RxSharp.Tests/Samples/QuickTasteSample.cs#L19-L27' title='Snippet source file'>snippet source</a> | <a href='#snippet-quick-taste-csharp' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## The example that started this project

[Puppeteer](https://pptr.dev/) (the JS browser-automation library) builds its `Locator.click()`/`.fill()` actions
on exactly one rxjs combinator: retry a flaky DOM action, racing the whole thing against a timeout and a
cancellation signal. Its own wrapper for this is `pipe(retry({delay}), raceWith(fromAbortSignal(...),
timeout(...)))` — three operators composed once, reused everywhere it needs "keep trying until this works, but
don't wait forever." Puppeteer's own .NET port, `puppeteer-sharp`, doesn't have that composition available, so
the equivalent logic there is a hand-rolled `while(true)` loop with a linked `CancellationTokenSource` and five
`catch` clauses to tell "timed out" apart from "cancelled" apart from "just retry."

RxSharp ports that exact combinator as `RetryAndRaceWithSignalAndTimer`, proven against a real launched Chrome:

<!-- snippet: retry-until-timeout-csharp -->
<a id='snippet-retry-until-timeout-csharp'></a>
```cs
// Retries a flaky async operation - "find an element that may not have rendered yet" is the
// Puppeteer case, but this works for any operation that fails until some condition is met -
// until it succeeds, times out, or the caller cancels.
public static async Task<string> FindElementOnceItRendersAsync(Func<Task<string>> tryFindElement, CancellationToken cancellationToken)
    => await Observable.Defer(() => Observable.From(tryFindElement()))
        .RetryAndRaceWithSignalAndTimer(TimeSpan.FromSeconds(5), cancellationToken)
        .FirstValueFrom().ConfigureAwait(false);
```
<sup><a href='https://github.com/hardkoded/ReactiveExtensions-Sharp/blob/main/test/RxSharp.Tests/Samples/RetryUntilTimeoutSample.cs#L13-L21' title='Snippet source file'>snippet source</a> | <a href='#snippet-retry-until-timeout-csharp' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

One line instead of a hand-rolled state machine — same retry-until-success-or-timeout-or-cancel behavior. See
[`handoff/puppeteer-sharp-integration.md`](handoff/puppeteer-sharp-integration.md) for the actual before/after
against puppeteer-sharp's real current `Locator` code, and how the rest of its rxjs-based internals
(`waitForNetworkIdle`, `ScreenRecorder`) map onto RxSharp.

## Install

```
dotnet add package ReactiveExtensionsSharp
```

Targets `netstandard2.0`, `net8.0`, and `net10.0`.

## Contributing

Every operator PR follows the same recipe: find its spec in upstream RxJS (`spec/operators/*-spec.ts` or `spec/observables/*-spec.ts` at tag `7.8.2`), port the test cases first, then implement until green. See [CLAUDE.md](CLAUDE.md) for the full set of conventions.

## License

MIT
