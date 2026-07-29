# ReactiveExtensionsSharp

A .NET port of [RxJS](https://rxjs.dev/) — same operators, same semantics, same names you already know, in idiomatic C#.

## Quick Start

```
dotnet add package ReactiveExtensionsSharp
```

```csharp
var clicks = Observable.FromEvent<EventArgs>(h => button.Click += h, h => button.Click -= h);

Observable.Interval(TimeSpan.FromSeconds(1))
    .Map(x => x * x)
    .Filter(x => x % 2 == 0)
    .TakeUntil(clicks)
    .Subscribe(x => Console.WriteLine(x));
```

## Links

* [GitHub](https://github.com/hardkoded/ReactiveExtensions-Sharp)
* [API Documentation](api/index.md)
* [Issues](https://github.com/hardkoded/ReactiveExtensions-Sharp/issues)
