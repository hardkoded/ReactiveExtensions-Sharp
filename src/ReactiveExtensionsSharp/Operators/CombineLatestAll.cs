namespace ReactiveExtensionsSharp.Operators;

/// <summary>Extension methods implementing the <c>combineLatestAll</c> operator.</summary>
public static class CombineLatestAllOperator
{
    /// <summary>
    /// Collects every inner observable produced by <paramref name="source"/> and, once <paramref name="source"/>
    /// completes, combines all of them with <see cref="ReactiveExtensionsSharp.Observable.CombineLatest{T}"/>: every time any
    /// collected inner observable emits (once every one of them has emitted at least once), the output emits a
    /// list of the latest values from each. Ported from rxjs's <c>joinAllInternals</c> applied to
    /// <c>combineLatest</c>: <c>source.ToArray().MergeMap(sources =&gt; CombineLatest(sources))</c>.
    /// </summary>
    /// <remarks>
    /// Same-type-only, like <see cref="ReactiveExtensionsSharp.Observable.CombineLatest{T}"/> itself: this does not port rxjs's
    /// optional <c>project</c> parameter (a heterogeneous-arity result selector), which has no clean C# equivalent
    /// for the same reason <c>Zip</c>/<c>ForkJoin</c>/<c>CombineLatest</c> stayed same-type-array-only. Apply
    /// <c>.Map(...)</c> to the result instead if a projection is needed.
    /// </remarks>
    /// <typeparam name="T">The type of values emitted by the inner observables.</typeparam>
    /// <param name="source">The higher-order source sequence.</param>
    /// <returns>An observable that emits a list of the latest values from every collected inner observable, updated as they emit.</returns>
    public static Observable<IReadOnlyList<T>> CombineLatestAll<T>(this Observable<Observable<T>> source)
        => source.ToArray().MergeMap(sources => ReactiveExtensionsSharp.Observable.CombineLatest(sources.ToArray()));
}
