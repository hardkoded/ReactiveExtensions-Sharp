namespace RxSharp.Operators;

/// <summary>Extension methods implementing the <c>concatAll</c> operator.</summary>
public static class ConcatAllOperator
{
    /// <summary>
    /// Converts a higher-order observable (an observable of observables) into a first-order observable by
    /// concatenating the inner observables in order: the next inner observable is only subscribed to once the
    /// previous one has completed. Equivalent to <c>source.ConcatMap(x =&gt; x)</c>.
    /// </summary>
    /// <typeparam name="T">The type of values emitted by the inner observables.</typeparam>
    /// <param name="source">The higher-order source sequence.</param>
    /// <returns>An observable that emits every inner observable's values in sequence, in source order.</returns>
    public static Observable<T> ConcatAll<T>(this Observable<Observable<T>> source) => source.ConcatMap(inner => inner);
}
