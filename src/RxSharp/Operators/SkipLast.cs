namespace RxSharp.Operators;

/// <summary>Implements the <c>SkipLast</c> operator. Mirrors rxjs's <c>skipLast</c>.</summary>
public static class SkipLastOperator
{
    /// <summary>
    /// Skips the last <paramref name="count"/> values emitted by <paramref name="source"/>: every value is held
    /// in an internal buffer until enough later values have arrived to prove it is not among the trailing
    /// <paramref name="count"/> values, at which point it is forwarded.
    /// </summary>
    /// <remarks>
    /// Every forwarded value is therefore delayed by <paramref name="count"/> emissions. If <paramref name="source"/>
    /// emits fewer than <paramref name="count"/> values before completing, nothing is ever forwarded. Unsubscribing
    /// does not flush the buffered values — they are simply discarded, matching rxjs's own <c>skipLast</c>.
    /// </remarks>
    /// <typeparam name="T">The element type of the source observable.</typeparam>
    /// <param name="source">The source observable to skip trailing values from.</param>
    /// <param name="count">The number of trailing values to skip. If zero or negative, <paramref name="source"/> is mirrored unchanged.</param>
    /// <returns>An observable of the values from <paramref name="source"/> with the last <paramref name="count"/> held back.</returns>
    public static Observable<T> SkipLast<T>(this Observable<T> source, int count)
    {
        if (count <= 0)
        {
            return source;
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
                        subscriber.OnNext(buffer.Dequeue());
                    }
                },
                onError: subscriber.OnError,
                onComplete: subscriber.OnCompleted);
        });
    }
}
