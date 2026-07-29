namespace ReactiveExtensionsSharp.Operators;

/// <summary>Implements the <c>TakeLast</c> operator. Mirrors rxjs's <c>takeLast</c>.</summary>
public static class TakeLastOperator
{
    /// <summary>
    /// Buffers up to the last <paramref name="count"/> values from <paramref name="source"/>, emitting all of
    /// them, in the order they were received, only once <paramref name="source"/> completes.
    /// </summary>
    /// <remarks>
    /// If <paramref name="source"/> emits fewer than <paramref name="count"/> values before completing, all of
    /// them are emitted. Because nothing is emitted until <paramref name="source"/> completes, using
    /// <see cref="TakeLast{T}(Observable{T}, int)"/> with a source that never completes results in an
    /// observable that never emits anything.
    /// </remarks>
    /// <typeparam name="T">The element type of the source observable.</typeparam>
    /// <param name="source">The source observable to buffer values from.</param>
    /// <param name="count">The maximum number of trailing values to emit.</param>
    /// <returns>
    /// An observable that emits at most the last <paramref name="count"/> values from <paramref name="source"/>,
    /// all at once, once it completes. If <paramref name="count"/> is zero or less, an empty, already-completed
    /// observable is returned and <paramref name="source"/> is never subscribed to.
    /// </returns>
    public static Observable<T> TakeLast<T>(this Observable<T> source, int count)
    {
        if (count <= 0)
        {
            return Observable.Empty<T>();
        }

        return source.Operate<T, T>((src, subscriber) =>
        {
            var buffer = new Queue<T>();

            return src.SubscribeChild(
                subscriber,
                onNext: value =>
                {
                    buffer.Enqueue(value);
                    if (buffer.Count > count)
                    {
                        buffer.Dequeue();
                    }
                },
                onError: subscriber.OnError,
                onComplete: () =>
                {
                    while (buffer.Count > 0)
                    {
                        subscriber.OnNext(buffer.Dequeue());
                    }

                    subscriber.OnCompleted();
                });
        });
    }
}
