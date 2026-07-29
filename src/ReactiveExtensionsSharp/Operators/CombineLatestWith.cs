namespace ReactiveExtensionsSharp.Operators;

/// <summary>Extension methods implementing the <c>combineLatestWith</c> operator.</summary>
public static class CombineLatestWithOperator
{
    /// <summary>
    /// Subscribes to <paramref name="source"/> and every one of <paramref name="otherSources"/>, emitting a list
    /// of the latest values from each once every one of them has emitted at least once, and again every time any
    /// of them emits thereafter. Pipeable-operator sugar over <see cref="ReactiveExtensionsSharp.Observable.CombineLatest{T}"/>
    /// with <paramref name="source"/> prepended: <c>source.CombineLatestWith(a, b)</c> is the same as
    /// <c>Observable.CombineLatest(source, a, b)</c>.
    /// </summary>
    /// <remarks>
    /// Same-type-only, like <see cref="ReactiveExtensionsSharp.Observable.CombineLatest{T}"/> itself, rather than rxjs's
    /// heterogeneously-typed tuple result.
    /// </remarks>
    /// <typeparam name="T">The type of values shared by <paramref name="source"/> and <paramref name="otherSources"/>.</typeparam>
    /// <param name="source">The source sequence, combined alongside <paramref name="otherSources"/>.</param>
    /// <param name="otherSources">The other sequences to combine with <paramref name="source"/>.</param>
    /// <returns>An observable that emits a list of the latest values from <paramref name="source"/> and every one of <paramref name="otherSources"/>.</returns>
    public static Observable<IReadOnlyList<T>> CombineLatestWith<T>(this Observable<T> source, params Observable<T>[] otherSources)
        => ReactiveExtensionsSharp.Observable.CombineLatest(Prepend(source, otherSources));

    private static Observable<T>[] Prepend<T>(Observable<T> source, Observable<T>[] otherSources)
    {
        var sources = new Observable<T>[otherSources.Length + 1];
        sources[0] = source;
        Array.Copy(otherSources, 0, sources, 1, otherSources.Length);
        return sources;
    }
}
