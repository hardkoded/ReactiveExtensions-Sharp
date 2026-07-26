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

## Status

Broad RxJS parity. 100+ operators and creation functions, every one ported test-for-test against [RxJS 7.8.2](https://github.com/ReactiveX/rxjs/tree/7.8.2)'s own spec suite — not reinvented from a changelog, the actual upstream test cases. 800+ tests, all green in CI.

- ✅ Core engine: `Observable<T>`, `Subscriber<T>`, `Subscription`, `Subject<T>`/`BehaviorSubject<T>`/`ReplaySubject<T>`/`AsyncSubject<T>`
- ✅ The full Puppeteer-essential surface — every creation function, operator, and async bridge (`firstValueFrom`/`lastValueFrom`) that [Puppeteer](https://pptr.dev/) itself funnels its rxjs usage through, plus `RxSharp.Extras` — hand-built combinators (`Timeout`, `RetryAndRaceWithSignalAndTimer`, `FromCancellationToken`, `FilterAsync`) matching how Puppeteer actually uses rxjs internally for retry/timeout/cancellation
- ✅ Validated end-to-end against a real, launched Chrome in a throwaway `puppeteer-sharp` playground branch — not just synthetic unit tests
- ✅ The long tail: `groupBy`, `share`/`shareReplay` (with the full config surface — `resetOnError`/`resetOnComplete`/`resetOnRefCountZero`/custom `connector`), the entire `window*`/`buffer*` families, `debounce`/`audit`/`sample`/`throttle` families, `bindCallback`/`bindNodeCallback`, `partition`, `using`, `onErrorResumeNext`, higher-order flattening (`mergeAll`/`switchAll`/`combineLatestAll`/`zipAll`/...), and more
- ✅ A `VirtualTimeScheduler` + marble-style `TestScheduler` for deterministic, virtual-time tests — the same testing idiom RxJS itself uses
- ✅ [API docs](https://hardkoded.github.io/ReactiveExtensions-Sharp/), published on every release
- 🚧 Optional, not blocking anything real: a full `Scheduler`/`Action` class hierarchy beyond `TaskPoolScheduler`/`VirtualTimeScheduler`, and standalone `connectable`/`publish`/`refCount` as their own API surface (`Share`/`ShareReplay` already cover the common case)

See [CLAUDE.md](CLAUDE.md) for the full milestone history and the architectural decisions behind the port.

## Install

```
dotnet add package ReactiveExtensionsSharp
```

Targets `netstandard2.0`, `net8.0`, and `net10.0`.

## Contributing

Every operator PR follows the same recipe: find its spec in upstream RxJS (`spec/operators/*-spec.ts` or `spec/observables/*-spec.ts` at tag `7.8.2`), port the test cases first, then implement until green. See [CLAUDE.md](CLAUDE.md) for the full set of conventions.

## License

MIT
