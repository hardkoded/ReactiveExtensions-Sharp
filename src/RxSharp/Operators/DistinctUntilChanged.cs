namespace RxSharp.Operators;

/// <summary>Implements the <c>DistinctUntilChanged</c> operator. Mirrors rxjs's <c>distinctUntilChanged</c>.</summary>
public static class DistinctUntilChangedOperator
{
    /// <summary>
    /// Emits a value from <paramref name="source"/> only when it differs from the immediately preceding emitted
    /// value, using <paramref name="comparer"/> (or the default equality comparer for <typeparamref name="T"/>).
    /// </summary>
    /// <remarks>
    /// The first value is always emitted. Comparison is only ever against the last value that was actually
    /// emitted downstream, not against every previously seen value — so a value can reappear later in the stream
    /// if something different was emitted in between (e.g. <c>1, 1, 2, 1</c> emits <c>1, 2, 1</c>).
    /// </remarks>
    /// <typeparam name="T">The element type of the source observable.</typeparam>
    /// <param name="source">The source observable to filter for consecutive distinct values.</param>
    /// <param name="comparer">The comparer used to test equality. Defaults to <see cref="EqualityComparer{T}.Default"/> when <see langword="null"/>.</param>
    /// <returns>An observable that suppresses consecutive duplicate values from <paramref name="source"/>.</returns>
    public static Observable<T> DistinctUntilChanged<T>(this Observable<T> source, IEqualityComparer<T>? comparer = null)
        => source.DistinctUntilChanged(comparer ?? EqualityComparer<T>.Default, x => x);

    /// <summary>
    /// Emits a value from <paramref name="source"/> only when the key computed for it by <paramref name="keySelector"/>
    /// differs, per <paramref name="comparer"/>, from the key computed for the immediately preceding emitted value.
    /// </summary>
    /// <remarks>
    /// The first value is always emitted (after having its key computed). <paramref name="keySelector"/> runs on
    /// every value, including the first. Only the computed key is compared and retained between emissions; the
    /// original value (not the key) is what gets emitted. If <paramref name="keySelector"/> throws, the exception
    /// is forwarded via <c>OnError</c> and the source subscription is torn down.
    /// </remarks>
    /// <typeparam name="T">The element type of the source observable.</typeparam>
    /// <typeparam name="TKey">The type of the key extracted from each value for comparison purposes.</typeparam>
    /// <param name="source">The source observable to filter for consecutive distinct keys.</param>
    /// <param name="comparer">The comparer used to test key equality.</param>
    /// <param name="keySelector">A function that extracts the comparison key from each value.</param>
    /// <returns>An observable that suppresses values whose key matches the previously emitted value's key.</returns>
    public static Observable<T> DistinctUntilChanged<T, TKey>(this Observable<T> source, IEqualityComparer<TKey> comparer, Func<T, TKey> keySelector)
        => source.Operate<T, T>((src, subscriber) =>
        {
            var first = true;
            var previousKey = default(TKey);

            return src.Subscribe(
                onNext: value =>
                {
                    TKey currentKey;
                    try
                    {
                        currentKey = keySelector(value);
                    }
                    catch (Exception ex)
                    {
                        subscriber.OnError(ex);
                        return;
                    }

                    if (first || !comparer.Equals(previousKey!, currentKey))
                    {
                        first = false;
                        previousKey = currentKey;
                        subscriber.OnNext(value);
                    }
                },
                onError: subscriber.OnError,
                onComplete: subscriber.OnCompleted);
        });
}
