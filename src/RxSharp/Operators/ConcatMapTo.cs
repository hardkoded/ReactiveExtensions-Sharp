namespace RxSharp.Operators;

/// <summary>Extension methods implementing the <c>concatMapTo</c> operator.</summary>
public static class ConcatMapToOperator
{
    /// <summary>
    /// Projects every value from <paramref name="source"/> to the same <paramref name="innerObservable"/>,
    /// regardless of the source value, concatenating each resulting subscription in order. Equivalent to
    /// <c>source.ConcatMap(_ =&gt; innerObservable)</c>.
    /// </summary>
    /// <remarks>
    /// Marked <c>@deprecated</c> upstream (in favor of <c>concatMap(() =&gt; result)</c>) but still present in rxjs
    /// 7.8.2, so it is ported here too. Like <see cref="ConcatMapOperator"/>, this does not port rxjs's deprecated
    /// <c>resultSelector</c> parameter — apply <c>.Map(...)</c> to the result for a projection.
    /// </remarks>
    /// <typeparam name="TSource">The type of values emitted by <paramref name="source"/>.</typeparam>
    /// <typeparam name="TResult">The type of values emitted by <paramref name="innerObservable"/>.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="innerObservable">The observable to subscribe to for every value emitted by <paramref name="source"/>.</param>
    /// <returns>An observable that concatenates the values of every subscription to <paramref name="innerObservable"/>, in source order.</returns>
    public static Observable<TResult> ConcatMapTo<TSource, TResult>(this Observable<TSource> source, Observable<TResult> innerObservable)
        => source.ConcatMap(_ => innerObservable);
}
