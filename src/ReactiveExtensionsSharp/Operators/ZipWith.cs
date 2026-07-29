namespace ReactiveExtensionsSharp.Operators;

/// <summary>Extension methods implementing the <c>zipWith</c> operator.</summary>
public static class ZipWithOperator
{
    /// <summary>
    /// Subscribes to <paramref name="source"/> and every one of <paramref name="otherSources"/>, combining the
    /// Nth value of each into a single emitted list, positionally. Pipeable-operator sugar over
    /// <see cref="ReactiveExtensionsSharp.Observable.Zip{T}"/> with <paramref name="source"/> prepended:
    /// <c>source.ZipWith(a, b)</c> is the same as <c>Observable.Zip(source, a, b)</c>.
    /// </summary>
    /// <remarks>
    /// Same-type-only, like <see cref="ReactiveExtensionsSharp.Observable.Zip{T}"/> itself, rather than rxjs's
    /// heterogeneously-typed tuple result.
    /// </remarks>
    /// <typeparam name="T">The type of values shared by <paramref name="source"/> and <paramref name="otherSources"/>.</typeparam>
    /// <param name="source">The source sequence, zipped alongside <paramref name="otherSources"/>.</param>
    /// <param name="otherSources">The other sequences to zip with <paramref name="source"/>.</param>
    /// <returns>An observable that emits a list of the Nth value from <paramref name="source"/> and every one of <paramref name="otherSources"/>, in order.</returns>
    public static Observable<IReadOnlyList<T>> ZipWith<T>(this Observable<T> source, params Observable<T>[] otherSources)
        => ReactiveExtensionsSharp.Observable.Zip(Prepend(source, otherSources));

    private static Observable<T>[] Prepend<T>(Observable<T> source, Observable<T>[] otherSources)
    {
        var sources = new Observable<T>[otherSources.Length + 1];
        sources[0] = source;
        Array.Copy(otherSources, 0, sources, 1, otherSources.Length);
        return sources;
    }
}
