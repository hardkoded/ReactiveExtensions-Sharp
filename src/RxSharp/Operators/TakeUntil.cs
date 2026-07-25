namespace RxSharp.Operators;

/// <summary>Extension methods implementing the <c>takeUntil</c> operator.</summary>
public static class TakeUntilOperator
{
    /// <summary>
    /// Mirrors <paramref name="source"/> until <paramref name="notifier"/> emits its first value, at which
    /// point the output completes and both <paramref name="source"/> and <paramref name="notifier"/> are
    /// unsubscribed. If <paramref name="notifier"/> never emits, all of <paramref name="source"/>'s values are
    /// passed through.
    /// </summary>
    /// <remarks>
    /// Only <paramref name="notifier"/>'s first <c>next</c> notification matters: if it errors or completes
    /// without ever emitting a value, that is ignored and has no effect on the output — it does not complete
    /// or error the output, matching rxjs's own <c>takeUntil</c> behavior.
    /// </remarks>
    /// <typeparam name="T">The type of values emitted by <paramref name="source"/> and by the output.</typeparam>
    /// <typeparam name="TNotifier">
    /// The type of values emitted by <paramref name="notifier"/>. Irrelevant to the output — only the fact
    /// that a value was emitted matters, not its contents.
    /// </typeparam>
    /// <param name="source">The source sequence to mirror.</param>
    /// <param name="notifier">The observable whose first emitted value causes the output to complete.</param>
    /// <returns>An observable that emits the values of <paramref name="source"/> until <paramref name="notifier"/> emits.</returns>
    public static Observable<T> TakeUntil<T, TNotifier>(this Observable<T> source, Observable<TNotifier> notifier)
        => source.Operate<T, T>((src, subscriber) =>
        {
            var notifierSubscription = notifier.Subscribe(onNext: _ => subscriber.OnCompleted());
            subscriber.Add(notifierSubscription);

            return subscriber.IsDisposed
                ? null
                : src.Subscribe(onNext: subscriber.OnNext, onError: subscriber.OnError, onComplete: subscriber.OnCompleted);
        });
}
