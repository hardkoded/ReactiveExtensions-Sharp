namespace RxSharp.Operators;

/// <summary>The <c>buffer</c> operator.</summary>
public static class BufferOperator
{
    /// <summary>
    /// Buffers <paramref name="source"/>'s values into an array, emitted each time <paramref name="closingNotifier"/>
    /// emits, then starts a new, empty buffer. Mirrors rxjs's <c>buffer</c>. Same boundary-observable idea as
    /// <see cref="WindowOperator.Window{T, TBoundary}"/>, but collecting into arrays instead of live windows:
    /// <paramref name="closingNotifier"/> is subscribed exactly once, for the whole lifetime of the output.
    /// If <paramref name="source"/> completes while a buffer is still open (regardless of whether it has any
    /// values), that buffer is emitted before completion is forwarded.
    /// </summary>
    /// <typeparam name="T">The type of the elements collected into each buffer.</typeparam>
    /// <typeparam name="TNotifier">The (unused) element type of <paramref name="closingNotifier"/>.</typeparam>
    /// <param name="source">The source observable to buffer.</param>
    /// <param name="closingNotifier">Each emission closes the current buffer (emitting it) and opens a new one.</param>
    /// <returns>An observable of each completed (or trailing) buffer as an <see cref="IReadOnlyList{T}"/>.</returns>
    public static Observable<IReadOnlyList<T>> Buffer<T, TNotifier>(this Observable<T> source, Observable<TNotifier> closingNotifier)
        => source.Operate<T, IReadOnlyList<T>>((src, subscriber) =>
        {
            var currentBuffer = new List<T>();

            var sourceSubscription = src.Subscribe(
                onNext: value => currentBuffer.Add(value),
                onError: subscriber.OnError,
                onComplete: () =>
                {
                    subscriber.OnNext(currentBuffer);
                    subscriber.OnCompleted();
                });
            subscriber.Add(sourceSubscription);

            var notifierSubscription = closingNotifier.Subscribe(
                onNext: _ =>
                {
                    var closed = currentBuffer;
                    currentBuffer = new List<T>();
                    subscriber.OnNext(closed);
                },
                onError: subscriber.OnError);
            subscriber.Add(notifierSubscription);

            return null;
        });
}
