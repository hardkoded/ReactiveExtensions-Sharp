namespace ReactiveExtensionsSharp.Operators;

/// <summary>Implements the <c>Partition</c> operator. Mirrors rxjs's <c>partition</c>.</summary>
public static class PartitionOperator
{
    /// <summary>
    /// Splits <paramref name="source"/> into two observables based on <paramref name="predicate"/>: one with the
    /// values for which it returns <see langword="true"/>, the other with the values for which it returns
    /// <see langword="false"/>.
    /// </summary>
    /// <remarks>
    /// Matches rxjs's actual implementation (<c>partition.ts</c>, verified against the 7.8.2 tag), which is
    /// literally <c>[filter(predicate)(source), filter(not(predicate))(source)]</c>: <paramref name="source"/> is
    /// subscribed to independently once per returned observable (twice overall, not shared), so each side has its
    /// own fully independent subscription lifetime, error path, and (for the indexed overload) index counter.
    /// If <paramref name="predicate"/> throws, both sides receive the exception via <c>OnError</c>, since each
    /// invokes it independently for the same value.
    /// </remarks>
    /// <typeparam name="T">The element type of the source observable.</typeparam>
    /// <param name="source">The source observable to split.</param>
    /// <param name="predicate">A function that tests each value.</param>
    /// <returns>A tuple of two observables: <c>Matched</c> for values satisfying <paramref name="predicate"/>, <c>Unmatched</c> for the rest.</returns>
    public static (Observable<T> Matched, Observable<T> Unmatched) Partition<T>(this Observable<T> source, Func<T, bool> predicate)
        => (source.Filter(predicate), source.Filter(value => !predicate(value)));

    /// <summary>
    /// Splits <paramref name="source"/> into two observables based on <paramref name="predicate"/>, called with
    /// each value and its zero-based emission index (tracked independently for each returned observable): one with
    /// the values for which it returns <see langword="true"/>, the other with the values for which it returns
    /// <see langword="false"/>.
    /// </summary>
    /// <remarks>
    /// Matches rxjs's actual implementation (<c>partition.ts</c>, verified against the 7.8.2 tag), which is
    /// literally <c>[filter(predicate)(source), filter(not(predicate))(source)]</c>: <paramref name="source"/> is
    /// subscribed to independently once per returned observable (twice overall, not shared), so each side has its
    /// own fully independent subscription lifetime, error path, and index counter. Since both sides observe the
    /// same source values in the same order, their index counters stay in lockstep, so the index seen by
    /// <paramref name="predicate"/> is the same for a given value regardless of which side is evaluating it.
    /// If <paramref name="predicate"/> throws, both sides receive the exception via <c>OnError</c>, since each
    /// invokes it independently for the same value.
    /// </remarks>
    /// <typeparam name="T">The element type of the source observable.</typeparam>
    /// <param name="source">The source observable to split.</param>
    /// <param name="predicate">A function that tests each value together with its index since subscription.</param>
    /// <returns>A tuple of two observables: <c>Matched</c> for values satisfying <paramref name="predicate"/>, <c>Unmatched</c> for the rest.</returns>
    public static (Observable<T> Matched, Observable<T> Unmatched) Partition<T>(this Observable<T> source, Func<T, int, bool> predicate)
        => (source.Filter(predicate), source.Filter((value, index) => !predicate(value, index)));
}
