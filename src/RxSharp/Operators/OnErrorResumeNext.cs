namespace RxSharp.Operators;

/// <summary>Extension methods implementing the pipeable-operator form of <c>onErrorResumeNext</c>.</summary>
public static class OnErrorResumeNextOperator
{
    /// <summary>
    /// Re-emits values from <paramref name="source"/>; once it ends — whether it completes or errors — subscribes
    /// to the first of <paramref name="otherSources"/> and continues the same way through the rest, in order,
    /// regardless of whether each one completes or errors. The output only completes once every source has been
    /// exhausted, and never itself errors. Pipeable-operator sugar over
    /// <see cref="RxSharp.Observable.OnErrorResumeNext{T}"/> with <paramref name="source"/> prepended:
    /// <c>source.OnErrorResumeNext(a, b)</c> is the same as <c>Observable.OnErrorResumeNext(source, a, b)</c>.
    /// </summary>
    /// <typeparam name="T">The type of values shared by <paramref name="source"/> and <paramref name="otherSources"/>.</typeparam>
    /// <param name="source">The source sequence, subscribed to first.</param>
    /// <param name="otherSources">The other sequences to subscribe to sequentially, in order, once <paramref name="source"/> ends (whether by completion or error).</param>
    /// <returns>An observable that emits every source's values in sequence, moving past errors instead of forwarding them, completing once every source has ended.</returns>
    public static Observable<T> OnErrorResumeNext<T>(this Observable<T> source, params Observable<T>[] otherSources)
        => RxSharp.Observable.OnErrorResumeNext(Prepend(source, otherSources));

    private static Observable<T>[] Prepend<T>(Observable<T> source, Observable<T>[] otherSources)
    {
        var sources = new Observable<T>[otherSources.Length + 1];
        sources[0] = source;
        Array.Copy(otherSources, 0, sources, 1, otherSources.Length);
        return sources;
    }
}
