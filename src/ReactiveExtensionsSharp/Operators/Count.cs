namespace ReactiveExtensionsSharp.Operators;

/// <summary>Implements the <c>Count</c> operator. Mirrors rxjs's <c>count</c>.</summary>
public static class CountOperator
{
    /// <summary>
    /// Emits a single integer: the total number of values emitted by <paramref name="source"/>, once it completes.
    /// </summary>
    /// <typeparam name="T">The element type of the source observable.</typeparam>
    /// <param name="source">The source observable to count.</param>
    /// <returns>An observable that emits the total value count, then completes.</returns>
    public static Observable<int> Count<T>(this Observable<T> source)
        => source.Count((_, _) => true);

    /// <summary>
    /// Emits a single integer: the number of values emitted by <paramref name="source"/> that satisfy
    /// <paramref name="predicate"/>, once <paramref name="source"/> completes.
    /// </summary>
    /// <remarks>If <paramref name="predicate"/> throws, the exception is forwarded via <c>OnError</c>.</remarks>
    /// <typeparam name="T">The element type of the source observable.</typeparam>
    /// <param name="source">The source observable to count.</param>
    /// <param name="predicate">A function that tests each value; only matching values are counted.</param>
    /// <returns>An observable that emits the matching value count, then completes.</returns>
    public static Observable<int> Count<T>(this Observable<T> source, Func<T, bool> predicate)
        => source.Count((value, _) => predicate(value));

    /// <summary>
    /// Emits a single integer: the number of values emitted by <paramref name="source"/> that satisfy
    /// <paramref name="predicate"/> (called with the value and its zero-based emission index), once
    /// <paramref name="source"/> completes.
    /// </summary>
    /// <remarks>If <paramref name="predicate"/> throws, the exception is forwarded via <c>OnError</c>.</remarks>
    /// <typeparam name="T">The element type of the source observable.</typeparam>
    /// <param name="source">The source observable to count.</param>
    /// <param name="predicate">A function that tests each value together with its index since subscription; only matching values are counted.</param>
    /// <returns>An observable that emits the matching value count, then completes.</returns>
    public static Observable<int> Count<T>(this Observable<T> source, Func<T, int, bool> predicate)
        => source.Operate<T, int>((src, subscriber) =>
        {
            var count = 0;
            var index = 0;

            return src.SubscribeChild(
                subscriber,
                onNext: value =>
                {
                    bool matches;
                    try
                    {
                        matches = predicate(value, index++);
                    }
                    catch (Exception ex)
                    {
                        subscriber.OnError(ex);
                        return;
                    }

                    if (matches)
                    {
                        count++;
                    }
                },
                onError: subscriber.OnError,
                onComplete: () =>
                {
                    subscriber.OnNext(count);
                    subscriber.OnCompleted();
                });
        });
}
