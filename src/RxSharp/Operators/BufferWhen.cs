namespace RxSharp.Operators;

/// <summary>The <c>bufferWhen</c> operator.</summary>
public static class BufferWhenOperator
{
    /// <summary>
    /// Buffers <paramref name="source"/>'s values into an array, emitted (and immediately replaced by a fresh,
    /// empty buffer) each time the observable returned by <paramref name="closingSelector"/> emits or completes.
    /// Mirrors rxjs's <c>bufferWhen</c>. Same closing-selector idea as
    /// <see cref="WindowWhenOperator.WindowWhen{T, TBoundary}"/>, but collecting into arrays instead of live windows;
    /// see its remarks for why each closing-notifier subscription is built directly via <see cref="Subscriber.Create{T}"/>
    /// assigned before subscribing, rather than a reassigned <see cref="SingleAssignmentDisposable"/>. The first
    /// buffer opens eagerly at subscription time; <paramref name="closingSelector"/> is invoked again, and its
    /// result subscribed to afresh, every time a new buffer opens.
    /// </summary>
    /// <typeparam name="T">The type of the elements collected into each buffer.</typeparam>
    /// <typeparam name="TBoundary">The (unused) element type of the closing-notifier observable.</typeparam>
    /// <param name="source">The source observable to buffer.</param>
    /// <param name="closingSelector">Invoked (with no arguments) each time a buffer opens; the observable it returns closes (and emits) that buffer on its first next or complete notification.</param>
    /// <returns>An observable of each completed buffer as an <see cref="IReadOnlyList{T}"/>.</returns>
    public static Observable<IReadOnlyList<T>> BufferWhen<T, TBoundary>(this Observable<T> source, Func<Observable<TBoundary>> closingSelector)
        => source.Operate<T, IReadOnlyList<T>>((src, subscriber) =>
        {
            List<T>? buffer = null;
            Subscriber<TBoundary>? closingSubscriber = null;

            void OpenBuffer()
            {
                closingSubscriber?.Dispose();

                var closed = buffer;
                buffer = new List<T>();
                if (closed is not null)
                {
                    subscriber.OnNext(closed);
                }

                Observable<TBoundary> closingNotifier;
                try
                {
                    closingNotifier = closingSelector();
                }
                catch (Exception ex)
                {
                    subscriber.OnError(ex);
                    return;
                }

                closingSubscriber = Subscriber.Create<TBoundary>(
                    onNext: _ => OpenBuffer(),
                    onError: subscriber.OnError,
                    onComplete: OpenBuffer);
                closingNotifier.Subscribe(closingSubscriber);
            }

            OpenBuffer();

            var sourceSubscription = src.Subscribe(
                onNext: value => buffer?.Add(value),
                onError: subscriber.OnError,
                onComplete: () =>
                {
                    if (buffer is not null)
                    {
                        subscriber.OnNext(buffer);
                    }

                    subscriber.OnCompleted();
                });
            subscriber.Add(sourceSubscription);

            return new Subscription(() => closingSubscriber?.Dispose());
        });
}
