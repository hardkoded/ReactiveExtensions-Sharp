namespace ReactiveExtensionsSharp.Operators;

/// <summary>Extension methods implementing the <c>switchMapTo</c> operator.</summary>
public static class SwitchMapToOperator
{
    /// <summary>
    /// Projects every value from <paramref name="source"/> to the same <paramref name="innerObservable"/>,
    /// regardless of the source value, switching to a fresh subscription every time <paramref name="source"/>
    /// emits (tearing down the previous one, even if still active). Equivalent to
    /// <c>source.SwitchMap(_ =&gt; innerObservable)</c>.
    /// </summary>
    /// <remarks>
    /// Marked <c>@deprecated</c> upstream (in favor of <c>switchMap(() =&gt; result)</c>) but still present in rxjs
    /// 7.8.2, so it is ported here too. Like <see cref="SwitchMapOperator"/>, this does not port rxjs's deprecated
    /// <c>resultSelector</c> parameter — apply <c>.Map(...)</c> to the result for a projection.
    /// </remarks>
    /// <typeparam name="TSource">The type of values emitted by <paramref name="source"/>.</typeparam>
    /// <typeparam name="TResult">The type of values emitted by <paramref name="innerObservable"/>.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="innerObservable">The observable to switch to for every value emitted by <paramref name="source"/>.</param>
    /// <returns>An observable that emits the values of only the most recent subscription to <paramref name="innerObservable"/>.</returns>
    public static Observable<TResult> SwitchMapTo<TSource, TResult>(this Observable<TSource> source, Observable<TResult> innerObservable)
        => source.SwitchMap(_ => innerObservable);
}
