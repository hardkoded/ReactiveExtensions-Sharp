namespace ReactiveExtensionsSharp.Operators;

/// <summary>Implements the <c>DefaultIfEmpty</c> operator. Mirrors rxjs's <c>defaultIfEmpty</c>.</summary>
public static class DefaultIfEmptyOperator
{
    /// <summary>
    /// Mirrors <paramref name="source"/>, but if it completes without ever emitting a value, emits
    /// <paramref name="defaultValue"/> first.
    /// </summary>
    /// <typeparam name="T">The element type of the source observable and the default value.</typeparam>
    /// <param name="source">The source observable to monitor for emptiness.</param>
    /// <param name="defaultValue">The value to emit if <paramref name="source"/> completes without emitting anything.</param>
    /// <returns>
    /// An observable that mirrors <paramref name="source"/>, substituting <paramref name="defaultValue"/> when it
    /// would otherwise complete empty.
    /// </returns>
    public static Observable<T> DefaultIfEmpty<T>(this Observable<T> source, T defaultValue)
        => source.Operate<T, T>((src, subscriber) =>
        {
            var hasValue = false;
            return src.SubscribeChild(
                subscriber,
                onNext: value =>
                {
                    hasValue = true;
                    subscriber.OnNext(value);
                },
                onError: subscriber.OnError,
                onComplete: () =>
                {
                    if (!hasValue)
                    {
                        subscriber.OnNext(defaultValue);
                    }

                    subscriber.OnCompleted();
                });
        });
}
