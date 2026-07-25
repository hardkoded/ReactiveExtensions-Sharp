# RxSharp

[![build](https://github.com/hardkoded/rxjs-sharp/actions/workflows/build.yml/badge.svg)](https://github.com/hardkoded/rxjs-sharp/actions/workflows/build.yml)
[![NuGet](https://img.shields.io/nuget/v/RxSharp.svg)](https://www.nuget.org/packages/RxSharp/)

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

```csharp
// RxSharp
var clicks = Observable.FromEvent(button, nameof(button.Click));

Observable.Interval(TimeSpan.FromSeconds(1))
    .Map(x => x * x)
    .Filter(x => x % 2 == 0)
    .TakeUntil(clicks)
    .Subscribe(x => Console.WriteLine(x));
```

## Status

Early and under active development. Ported test-for-test against [RxJS 7.8.2](https://github.com/ReactiveX/rxjs/tree/7.8.2)'s own spec suite, operator by operator.

- ✅ Core engine: `Observable<T>`, `Subscriber<T>`, `Subscription`, `Subject<T>`, `ReplaySubject<T>`
- 🚧 Broad operator coverage (transformation, filtering, combination, error handling)
- 🚧 `RxSharp.Extras` — combinators for real-world async/cancellation patterns (timeout, retry+race, event bridging), lifted from how [Puppeteer](https://pptr.dev/) itself uses rxjs internally
- ⏳ Schedulers, marble/`TestScheduler`-style testing, full parity long tail

See open issues for what's actively being ported next.

## Install

```
dotnet add package RxSharp
```

(Not yet published — coming with the first tagged release.)

## Contributing

Every operator PR follows the same recipe: find its spec in upstream RxJS (`spec/operators/*-spec.ts` or `spec/observables/*-spec.ts` at tag `7.8.2`), port the test cases first, then implement until green. See `CLAUDE.md` for the full set of conventions.

## License

MIT
