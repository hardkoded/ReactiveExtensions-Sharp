namespace ReactiveExtensionsSharp.Operators;

/// <summary>Implements the <c>FindIndex</c> operator. Mirrors rxjs's <c>findIndex</c>.</summary>
public static class FindIndexOperator
{
    /// <summary>
    /// Emits only the zero-based index of the first value from <paramref name="source"/> that satisfies
    /// <paramref name="predicate"/>, then completes.
    /// </summary>
    /// <remarks>
    /// Like <see cref="FindOperator.Find{T}(Observable{T}, Func{T, bool})"/>, but emits the index of the match
    /// instead of the value itself. If no value satisfies <paramref name="predicate"/> before <paramref name="source"/>
    /// completes, the result emits <c>-1</c> and completes normally (no error). If <paramref name="predicate"/>
    /// throws, the exception is forwarded via <c>OnError</c>.
    /// </remarks>
    /// <typeparam name="T">The element type of the source observable.</typeparam>
    /// <param name="source">The source observable to search.</param>
    /// <param name="predicate">A function that tests each value.</param>
    /// <returns>An observable of the index of the first matching value, or <c>-1</c> if none is found.</returns>
    public static Observable<int> FindIndex<T>(this Observable<T> source, Func<T, bool> predicate)
        => source.FindIndex((value, _) => predicate(value));

    /// <summary>
    /// Emits only the zero-based index of the first value from <paramref name="source"/> that satisfies
    /// <paramref name="predicate"/> (called with the value and its zero-based emission index), then completes.
    /// </summary>
    /// <remarks>
    /// Like <see cref="FindOperator.Find{T}(Observable{T}, Func{T, int, bool})"/>, but emits the index of the
    /// match instead of the value itself. If no value satisfies <paramref name="predicate"/> before <paramref name="source"/>
    /// completes, the result emits <c>-1</c> and completes normally (no error). If <paramref name="predicate"/>
    /// throws, the exception is forwarded via <c>OnError</c>.
    /// </remarks>
    /// <typeparam name="T">The element type of the source observable.</typeparam>
    /// <param name="source">The source observable to search.</param>
    /// <param name="predicate">A function that tests each value together with its index since subscription.</param>
    /// <returns>An observable of the index of the first matching value, or <c>-1</c> if none is found.</returns>
    public static Observable<int> FindIndex<T>(this Observable<T> source, Func<T, int, bool> predicate)
        => source.Operate<T, int>((src, subscriber)
            => FindCore.Subscribe(src, subscriber, predicate, onMatch: (_, i) => i, noMatchResult: -1));
}
