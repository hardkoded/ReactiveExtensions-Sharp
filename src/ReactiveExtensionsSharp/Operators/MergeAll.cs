namespace ReactiveExtensionsSharp.Operators;

/// <summary>Extension methods implementing the <c>mergeAll</c> operator.</summary>
public static class MergeAllOperator
{
    /// <summary>
    /// Converts a higher-order observable (an observable of observables) into a first-order observable by
    /// subscribing to every inner observable as soon as it is produced and merging all of their emissions into
    /// the output, with unbounded concurrency. Equivalent to <c>source.MergeMap(x =&gt; x)</c>.
    /// </summary>
    /// <typeparam name="T">The type of values emitted by the inner observables.</typeparam>
    /// <param name="source">The higher-order source sequence.</param>
    /// <returns>An observable that merges the values of every inner observable produced by <paramref name="source"/>.</returns>
    public static Observable<T> MergeAll<T>(this Observable<Observable<T>> source) => source.MergeMap(inner => inner);
}
