namespace RxSharp.Operators;

/// <summary>Extension methods implementing the <c>max</c> operator.</summary>
/// <remarks>
/// Built directly on the no-seed <see cref="ReduceOperator.Reduce{T}(Observable{T}, Func{T, T, T})"/> overload,
/// matching rxjs's own <c>max.ts</c> (itself a thin wrapper over <c>reduce</c>). Since that overload emits nothing
/// for an empty source rather than erroring, so does <see cref="Max{T}(Observable{T})"/> — matches rxjs exactly, verified
/// against the 7.8.2 tag rather than assumed.
/// </remarks>
public static class MaxOperator
{
    /// <summary>
    /// Emits the single largest value seen from <paramref name="source"/>, once it completes, using the default
    /// <see cref="Comparer{T}"/> for <typeparamref name="T"/>.
    /// </summary>
    /// <remarks>If <paramref name="source"/> completes without ever emitting, the output completes without emitting any value either.</remarks>
    /// <typeparam name="T">The type of values emitted by <paramref name="source"/> and of the output.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <returns>An observable that emits the largest value seen, once <paramref name="source"/> completes.</returns>
    public static Observable<T> Max<T>(this Observable<T> source)
        => source.Max(Comparer<T>.Default.Compare);

    /// <summary>
    /// Emits the single largest value seen from <paramref name="source"/> (per <paramref name="comparer"/>),
    /// once it completes.
    /// </summary>
    /// <remarks>
    /// If <paramref name="source"/> completes without ever emitting, the output completes without emitting any
    /// value either. If <paramref name="comparer"/> throws, the exception is forwarded via <c>OnError</c>.
    /// </remarks>
    /// <typeparam name="T">The type of values emitted by <paramref name="source"/> and of the output.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="comparer">A function returning a positive number if the first value is greater, matching <see cref="Comparer{T}.Compare(T, T)"/>'s contract.</param>
    /// <returns>An observable that emits the largest value seen, once <paramref name="source"/> completes.</returns>
    public static Observable<T> Max<T>(this Observable<T> source, Func<T, T, int> comparer)
        => source.Reduce((x, y) => comparer(x, y) > 0 ? x : y);
}
