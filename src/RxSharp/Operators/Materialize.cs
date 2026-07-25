namespace RxSharp.Operators;

/// <summary>Extension methods implementing the <c>materialize</c> and <c>dematerialize</c> operators.</summary>
public static class MaterializeOperator
{
    /// <summary>
    /// Converts each next/error/complete notification from <paramref name="source"/> into an explicit
    /// <see cref="Notification{T}"/> value emitted through the stream, so e.g. an error becomes a value rather
    /// than terminating the stream. The output itself always completes normally: a <see cref="Notification{T}"/>
    /// wrapping the error (or the completion) is emitted first, immediately followed by <c>OnCompleted</c>.
    /// </summary>
    /// <typeparam name="T">The type of values emitted by <paramref name="source"/>.</typeparam>
    /// <param name="source">The source sequence to materialize.</param>
    /// <returns>
    /// An observable of <see cref="Notification{T}"/> values: one <see cref="NotificationKind.Next"/> per value
    /// from <paramref name="source"/>, followed by a single terminal <see cref="NotificationKind.Error"/> or
    /// <see cref="NotificationKind.Completed"/> notification and then <c>OnCompleted</c>.
    /// </returns>
    public static Observable<Notification<T>> Materialize<T>(this Observable<T> source)
        => source.Operate<T, Notification<T>>((src, subscriber) =>
            src.SubscribeChild(
                subscriber,
                onNext: value => subscriber.OnNext(Notification.CreateNext(value)),
                onError: error =>
                {
                    subscriber.OnNext(Notification.CreateError<T>(error));
                    subscriber.OnCompleted();
                },
                onComplete: () =>
                {
                    subscriber.OnNext(Notification.CreateCompleted<T>());
                    subscriber.OnCompleted();
                }));

    /// <summary>
    /// Converts each <see cref="Notification{T}"/> value emitted by <paramref name="source"/> back into a real
    /// next/error/complete notification, the inverse of <see cref="Materialize{T}"/>. A <see cref="NotificationKind.Next"/>
    /// value is unwrapped and forwarded via <c>OnNext</c>; a <see cref="NotificationKind.Error"/> value terminates
    /// the output via <c>OnError</c>; a <see cref="NotificationKind.Completed"/> value terminates the output via
    /// <c>OnCompleted</c>. If <paramref name="source"/> itself errors or completes without emitting a matching
    /// notification value, that is forwarded unchanged.
    /// </summary>
    /// <typeparam name="T">The type of values carried by the <see cref="Notification{T}"/> values from <paramref name="source"/>.</typeparam>
    /// <param name="source">The source sequence of notifications to dematerialize.</param>
    /// <returns>An observable of the unwrapped values, terminating as directed by each notification.</returns>
    public static Observable<T> Dematerialize<T>(this Observable<Notification<T>> source)
        => source.Operate<Notification<T>, T>((src, subscriber) =>
            src.SubscribeChild(
                subscriber,
                onNext: notification => notification.Accept(subscriber),
                onError: subscriber.OnError,
                onComplete: subscriber.OnCompleted));
}
