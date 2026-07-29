namespace ReactiveExtensionsSharp.Operators;

/// <summary>Implements the <c>Distinct</c> operator. Mirrors rxjs's <c>distinct</c>.</summary>
public static class DistinctOperator
{
    /// <summary>
    /// Emits a value from <paramref name="source"/> only if it has not been seen before, anywhere in the stream
    /// since subscription &#8212; using <paramref name="comparer"/> (or the default equality comparer for
    /// <typeparamref name="T"/>).
    /// </summary>
    /// <remarks>
    /// Unlike <see cref="DistinctUntilChangedOperator.DistinctUntilChanged{T}(Observable{T}, IEqualityComparer{T}?)"/>,
    /// which only compares a value against the immediately preceding one, this operator tracks every distinct
    /// value ever seen (in an internal set) for the lifetime of the subscription &#8212; so a repeated value is
    /// suppressed no matter how long ago it last appeared.
    /// </remarks>
    /// <typeparam name="T">The element type of the source observable.</typeparam>
    /// <param name="source">The source observable to filter for distinct values.</param>
    /// <param name="comparer">The comparer used to test equality. Defaults to <see cref="EqualityComparer{T}.Default"/> when <see langword="null"/>.</param>
    /// <returns>An observable that emits only values from <paramref name="source"/> not previously emitted.</returns>
    public static Observable<T> Distinct<T>(this Observable<T> source, IEqualityComparer<T>? comparer = null)
        => source.Distinct(x => x, comparer);

    /// <summary>
    /// Emits a value from <paramref name="source"/> only if the key computed for it by <paramref name="keySelector"/>
    /// has not been computed before, anywhere in the stream since subscription &#8212; using <paramref name="comparer"/>
    /// (or the default equality comparer for <typeparamref name="TKey"/>).
    /// </summary>
    /// <remarks>
    /// The original value (not the key) is what gets emitted. If <paramref name="keySelector"/> throws, the
    /// exception is forwarded via <c>OnError</c> and the source subscription is torn down.
    /// </remarks>
    /// <typeparam name="T">The element type of the source observable.</typeparam>
    /// <typeparam name="TKey">The type of the key extracted from each value for distinctness tracking.</typeparam>
    /// <param name="source">The source observable to filter for distinct keys.</param>
    /// <param name="keySelector">A function that extracts the comparison key from each value.</param>
    /// <param name="comparer">The comparer used to test key equality. Defaults to <see cref="EqualityComparer{TKey}.Default"/> when <see langword="null"/>.</param>
    /// <returns>An observable that emits only values from <paramref name="source"/> whose key was not previously seen.</returns>
    public static Observable<T> Distinct<T, TKey>(this Observable<T> source, Func<T, TKey> keySelector, IEqualityComparer<TKey>? comparer = null)
        => source.Operate<T, T>((src, subscriber) =>
        {
            var seenKeys = new HashSet<TKey>(comparer ?? EqualityComparer<TKey>.Default);

            // Built directly and registered as a child of `subscriber` *before* subscribing to `src` --
            // deliberately not the plain Subscribe(onNext:...) convenience overload, whose returned disposable is
            // only registered as a child of `subscriber` *after* Subscribe returns. This operator never completes
            // early on its own, but a downstream operator (e.g. Take) might, and for a fully-synchronous source
            // Subscribe doesn't return until the source is exhausted -- so the late registration would make a
            // downstream early-unsubscribe unable to cascade back through this operator in time (same root cause
            // as the Take.cs/First.cs hazard, one layer removed: the early-stop decision is made downstream here,
            // not inside this operator itself).
            Subscriber<T> innerSubscriber = null!;
            innerSubscriber = Subscriber.Create<T>(
                onNext: value =>
                {
                    TKey key;
                    try
                    {
                        key = keySelector(value);
                    }
                    catch (Exception ex)
                    {
                        subscriber.OnError(ex);
                        return;
                    }

                    if (seenKeys.Add(key))
                    {
                        subscriber.OnNext(value);
                    }
                },
                onError: subscriber.OnError,
                onComplete: subscriber.OnCompleted);

            subscriber.Add(innerSubscriber);
            src.Subscribe(innerSubscriber);
            return null;
        });
}
