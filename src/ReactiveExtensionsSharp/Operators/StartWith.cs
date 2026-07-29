namespace ReactiveExtensionsSharp.Operators;

/// <summary>Extension methods implementing the <c>startWith</c> operator.</summary>
public static class StartWithOperator
{
    /// <summary>
    /// Returns an observable that, at the moment of subscription, synchronously emits <paramref name="values"/>
    /// in order, and only then subscribes to <paramref name="source"/> and mirrors its emissions.
    /// </summary>
    /// <remarks>
    /// If the subscriber is disposed while the seed values are still being emitted (for example, a downstream
    /// operator like <c>Take</c> unsubscribes after the first value), emission of the remaining seed values
    /// stops immediately and <paramref name="source"/> is never subscribed to.
    /// </remarks>
    /// <typeparam name="T">The type of values in <paramref name="values"/> and emitted by <paramref name="source"/>.</typeparam>
    /// <param name="source">The source sequence to emit after the seed values.</param>
    /// <param name="values">The values to emit, in order, before subscribing to <paramref name="source"/>.</param>
    /// <returns>An observable that emits <paramref name="values"/> followed by the emissions of <paramref name="source"/>.</returns>
    public static Observable<T> StartWith<T>(this Observable<T> source, params T[] values)
        => source.Operate<T, T>((src, subscriber) =>
        {
            foreach (var value in values)
            {
                if (subscriber.IsDisposed)
                {
                    return null;
                }

                subscriber.OnNext(value);
            }

            return subscriber.IsDisposed
                ? null
                : src.SubscribeChild(subscriber, onNext: subscriber.OnNext, onError: subscriber.OnError, onComplete: subscriber.OnCompleted);
        });
}
