namespace ReactiveExtensionsSharp.Operators;

/// <summary>Extension methods implementing the <c>mapTo</c> operator.</summary>
public static class MapToOperator
{
    /// <summary>
    /// Emits <paramref name="value"/> every time <paramref name="source"/> emits, ignoring the actual source
    /// value. Equivalent to <c>source.Map(_ =&gt; value)</c>. Errors and completion are passed through unchanged.
    /// </summary>
    /// <remarks>Marked <c>@deprecated</c> upstream (in favor of <c>map(() =&gt; value)</c>) but still present in rxjs 7.8.2, so it is ported here too.</remarks>
    /// <typeparam name="TSource">The type of values emitted by <paramref name="source"/>.</typeparam>
    /// <typeparam name="TResult">The type of <paramref name="value"/>.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="value">The constant value to emit in place of every source value.</param>
    /// <returns>An observable that emits <paramref name="value"/> once for every value <paramref name="source"/> emits.</returns>
    public static Observable<TResult> MapTo<TSource, TResult>(this Observable<TSource> source, TResult value)
        => source.Map(_ => value);
}
