namespace RxSharp.Operators;

/// <summary>Extension methods implementing the <c>zipAll</c> operator.</summary>
public static class ZipAllOperator
{
    /// <summary>
    /// Collects every inner observable produced by <paramref name="source"/> and, once <paramref name="source"/>
    /// completes, combines all of them with <see cref="RxSharp.Observable.Zip{T}"/>: the Nth value from every
    /// collected inner observable is combined into a single emitted list, positionally. Ported from rxjs's
    /// <c>joinAllInternals</c> applied to <c>zip</c>: <c>source.ToArray().MergeMap(sources =&gt; Zip(sources))</c>.
    /// </summary>
    /// <remarks>
    /// Same-type-only, like <see cref="RxSharp.Observable.Zip{T}"/> itself — see <see cref="CombineLatestAllOperator.CombineLatestAll{T}"/>'s
    /// remarks for why no <c>project</c> parameter is ported.
    /// </remarks>
    /// <typeparam name="T">The type of values emitted by the inner observables.</typeparam>
    /// <param name="source">The higher-order source sequence.</param>
    /// <returns>An observable that emits a list of the Nth value from every collected inner observable, in order.</returns>
    public static Observable<IReadOnlyList<T>> ZipAll<T>(this Observable<Observable<T>> source)
        => source.ToArray().MergeMap(sources => RxSharp.Observable.Zip(sources.ToArray()));
}
