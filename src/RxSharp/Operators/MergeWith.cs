namespace RxSharp.Operators;

/// <summary>Extension methods implementing the <c>mergeWith</c> operator.</summary>
public static class MergeWithOperator
{
    /// <summary>
    /// Subscribes to <paramref name="source"/> and every one of <paramref name="otherSources"/> at the same
    /// time, emitting every value from every one of them as it arrives. Pipeable-operator sugar over
    /// <see cref="RxSharp.Observable.Merge{T}"/> with <paramref name="source"/> prepended:
    /// <c>source.MergeWith(a, b)</c> is the same as <c>Observable.Merge(source, a, b)</c>.
    /// </summary>
    /// <typeparam name="T">The type of values shared by <paramref name="source"/> and <paramref name="otherSources"/>.</typeparam>
    /// <param name="source">The source sequence, merged alongside <paramref name="otherSources"/>.</param>
    /// <param name="otherSources">The other sequences to merge with <paramref name="source"/>.</param>
    /// <returns>An observable that emits the interleaved values of <paramref name="source"/> and every one of <paramref name="otherSources"/>.</returns>
    public static Observable<T> MergeWith<T>(this Observable<T> source, params Observable<T>[] otherSources)
        => RxSharp.Observable.Merge(Prepend(source, otherSources));

    private static Observable<T>[] Prepend<T>(Observable<T> source, Observable<T>[] otherSources)
    {
        var sources = new Observable<T>[otherSources.Length + 1];
        sources[0] = source;
        Array.Copy(otherSources, 0, sources, 1, otherSources.Length);
        return sources;
    }
}
