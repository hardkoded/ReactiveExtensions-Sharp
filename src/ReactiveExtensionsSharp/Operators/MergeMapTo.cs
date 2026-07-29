namespace ReactiveExtensionsSharp.Operators;

/// <summary>Extension methods implementing the <c>mergeMapTo</c> operator.</summary>
public static class MergeMapToOperator
{
    /// <summary>
    /// Projects every value from <paramref name="source"/> to the same <paramref name="innerObservable"/>,
    /// regardless of the source value, merging every resulting subscription's emissions into the output with
    /// unbounded concurrency. Equivalent to <c>source.MergeMap(_ =&gt; innerObservable)</c>.
    /// </summary>
    /// <remarks>
    /// Marked <c>@deprecated</c> upstream (in favor of <c>mergeMap(() =&gt; result)</c>) but still present in rxjs
    /// 7.8.2, so it is ported here too. Like <see cref="MergeMapOperator"/>, this does not port rxjs's deprecated
    /// <c>resultSelector</c>/<c>concurrent</c> parameters — apply <c>.Map(...)</c> to the result for a projection.
    /// </remarks>
    /// <typeparam name="TSource">The type of values emitted by <paramref name="source"/>.</typeparam>
    /// <typeparam name="TResult">The type of values emitted by <paramref name="innerObservable"/>.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="innerObservable">The observable to subscribe to for every value emitted by <paramref name="source"/>.</param>
    /// <returns>An observable that merges the values of every subscription to <paramref name="innerObservable"/>.</returns>
    public static Observable<TResult> MergeMapTo<TSource, TResult>(this Observable<TSource> source, Observable<TResult> innerObservable)
        => source.MergeMap(_ => innerObservable);
}
