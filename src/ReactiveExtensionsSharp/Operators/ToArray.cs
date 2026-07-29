namespace ReactiveExtensionsSharp.Operators;

/// <summary>Implements the <c>ToArray</c> operator. Mirrors rxjs's <c>toArray</c>.</summary>
public static class ToArrayOperator
{
    /// <summary>
    /// Buffers every value emitted by <paramref name="source"/> and, once it completes, emits them all as a
    /// single list, then completes.
    /// </summary>
    /// <remarks>
    /// If <paramref name="source"/> errors, no list is emitted &#8212; the error is forwarded directly instead.
    /// </remarks>
    /// <typeparam name="T">The element type of the source observable.</typeparam>
    /// <param name="source">The source observable to buffer.</param>
    /// <returns>An observable that emits a single list of every value from <paramref name="source"/>, then completes.</returns>
    public static Observable<IReadOnlyList<T>> ToArray<T>(this Observable<T> source)
        => source.Operate<T, IReadOnlyList<T>>((src, subscriber) =>
        {
            var buffer = new List<T>();

            return src.SubscribeChild(
                subscriber,
                onNext: buffer.Add,
                onError: subscriber.OnError,
                onComplete: () =>
                {
                    subscriber.OnNext(buffer);
                    subscriber.OnCompleted();
                });
        });
}
