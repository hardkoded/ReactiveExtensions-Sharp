using ReactiveExtensionsSharp.Operators;

namespace ReactiveExtensionsSharp.Tests.Samples;

/// <summary>
/// Not an NUnit test: a real, compiled source for the README's "pipe" example. Kept in sync with the README via
/// MarkdownSnippets, same as <see cref="QuickTasteSample"/>.
/// </summary>
public static class PipeSample
{
    // begin-snippet: pipe-csharp
    public static void Run(Observable<int> numbersA, Observable<int> numbersB)
    {
        // A reusable transformation, defined once - the equivalent of RxJS's standalone
        // `const squareAndFilterEven = pipe(map(x => x * x), filter(x => x % 2 === 0));`
        OperatorFunction<int, int> squareAndFilterEven = source => source.Map(x => x * x).Filter(x => x % 2 == 0);

        numbersA.Pipe(squareAndFilterEven).Subscribe(x => Console.WriteLine(x));
        numbersB.Pipe(squareAndFilterEven).Subscribe(x => Console.WriteLine(x));
    }
    // end-snippet
}
