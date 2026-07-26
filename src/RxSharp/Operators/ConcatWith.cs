namespace RxSharp.Operators;

/// <summary>Extension methods implementing the <c>concatWith</c> operator.</summary>
public static class ConcatWithOperator
{
    /// <summary>
    /// Emits all values from <paramref name="source"/>, then, once it completes, subscribes to each of
    /// <paramref name="otherSources"/> in turn, one at a time. Pipeable-operator sugar over
    /// <see cref="RxSharp.Observable.Concat{T}"/> with <paramref name="source"/> prepended:
    /// <c>source.ConcatWith(a, b)</c> is the same as <c>Observable.Concat(source, a, b)</c>.
    /// </summary>
    /// <typeparam name="T">The type of values shared by <paramref name="source"/> and <paramref name="otherSources"/>.</typeparam>
    /// <param name="source">The source sequence, subscribed to first.</param>
    /// <param name="otherSources">The other sequences to subscribe to sequentially, in order, after <paramref name="source"/> completes.</param>
    /// <returns>An observable that emits <paramref name="source"/>'s values followed by each of <paramref name="otherSources"/>'s, in order.</returns>
    public static Observable<T> ConcatWith<T>(this Observable<T> source, params Observable<T>[] otherSources)
        => RxSharp.Observable.Concat(Prepend(source, otherSources));

    private static Observable<T>[] Prepend<T>(Observable<T> source, Observable<T>[] otherSources)
    {
        var sources = new Observable<T>[otherSources.Length + 1];
        sources[0] = source;
        Array.Copy(otherSources, 0, sources, 1, otherSources.Length);
        return sources;
    }
}
