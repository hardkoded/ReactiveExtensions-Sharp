namespace RxSharp.Operators;

/// <summary>Extension methods implementing the <c>skipUntil</c> operator.</summary>
public static class SkipUntilOperator
{
    /// <summary>
    /// Skips values from <paramref name="source"/> until <paramref name="notifier"/> emits its first value, at
    /// which point every value after that (including ones emitted by <paramref name="source"/> after the switch)
    /// is forwarded. The inverse of <see cref="TakeUntilOperator.TakeUntil{T, TNotifier}"/>.
    /// </summary>
    /// <remarks>
    /// <paramref name="notifier"/> is unsubscribed as soon as it emits its first value — later values it might
    /// emit have no further effect. If <paramref name="notifier"/> completes without ever emitting a value, the
    /// output never forwards anything from <paramref name="source"/> (though it still mirrors its completion or
    /// error). If <paramref name="notifier"/> errors, that error is forwarded to the output, matching rxjs's own
    /// <c>skipUntil</c> — unlike <see cref="TakeUntilOperator.TakeUntil{T, TNotifier}"/>, a notifier error is not
    /// silently ignored here.
    /// </remarks>
    /// <typeparam name="T">The type of values emitted by <paramref name="source"/> and by the output.</typeparam>
    /// <typeparam name="TNotifier">
    /// The type of values emitted by <paramref name="notifier"/>. Irrelevant to the output — only the fact
    /// that a value was emitted matters, not its contents.
    /// </typeparam>
    /// <param name="source">The source sequence to skip values from.</param>
    /// <param name="notifier">The observable whose first emitted value causes the output to start forwarding <paramref name="source"/>'s values.</param>
    /// <returns>An observable that skips <paramref name="source"/>'s values until <paramref name="notifier"/> emits, then forwards the rest.</returns>
    public static Observable<T> SkipUntil<T, TNotifier>(this Observable<T> source, Observable<TNotifier> notifier)
        => source.Operate<T, T>((src, subscriber) =>
        {
            var taking = false;

            // Built directly (rather than via the Subscribe(onNext:...) convenience overload) and registered as
            // a child of `subscriber` before `notifier.Subscribe` runs, so a synchronous notifier that emits more
            // than one value (or keeps emitting after its first) is stopped by self-disposal immediately, not only
            // once the whole call stack unwinds. See CLAUDE.md's Learnings for the general pattern this follows.
            Subscriber<TNotifier>? notifierSubscriber = null;
            notifierSubscriber = Subscriber.Create<TNotifier>(
                onNext: _ =>
                {
                    taking = true;
                    notifierSubscriber!.Dispose();
                },
                onError: subscriber.OnError);
            subscriber.Add(notifierSubscriber);
            notifier.Subscribe(notifierSubscriber);

            return subscriber.IsDisposed
                ? null
                : src.SubscribeChild(
                    subscriber,
                    onNext: value =>
                    {
                        if (taking)
                        {
                            subscriber.OnNext(value);
                        }
                    },
                    onError: subscriber.OnError,
                    onComplete: subscriber.OnCompleted);
        });
}
