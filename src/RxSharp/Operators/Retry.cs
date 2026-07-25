namespace RxSharp.Operators;

/// <summary>Extension methods implementing the <c>retry</c> operator.</summary>
public static class RetryOperator
{
    /// <summary>
    /// Resubscribes to <paramref name="source"/> whenever it errors, up to <paramref name="count"/> times,
    /// instead of forwarding the error downstream. Once the retry budget is exhausted, the most recent error
    /// is forwarded via <c>OnError</c> as usual. Values emitted before a failed attempt are still forwarded
    /// downstream; a retry only affects what happens after the error.
    /// </summary>
    /// <remarks>
    /// This is a reduced form of rxjs's <c>retry</c>: it supports a retry count and, optionally, a single fixed
    /// delay before each resubscription (via a <c>Timer</c> on <paramref name="scheduler"/>), but not rxjs's
    /// full <c>RetryConfig</c> object — there is no <c>resetOnSuccess</c> option, and <paramref name="delay"/>
    /// cannot be a per-error notifier function, only a constant <see cref="TimeSpan"/>.
    /// </remarks>
    /// <typeparam name="T">The type of values emitted by <paramref name="source"/>.</typeparam>
    /// <param name="source">The source sequence to retry on error.</param>
    /// <param name="count">
    /// The maximum number of resubscriptions to attempt after an error. Defaults to <see cref="int.MaxValue"/>
    /// (effectively unlimited retries). A value of zero or less disables retrying entirely, and <paramref name="source"/>
    /// is returned unchanged.
    /// </param>
    /// <param name="delay">
    /// An optional fixed delay to wait before each resubscription. If <see langword="null"/> (the default),
    /// resubscription happens immediately after the error.
    /// </param>
    /// <param name="scheduler">The scheduler used to time <paramref name="delay"/>, if provided.</param>
    /// <returns>An observable that mirrors <paramref name="source"/>, retrying on error as described above.</returns>
    public static Observable<T> Retry<T>(this Observable<T> source, int count = int.MaxValue, TimeSpan? delay = null, IScheduler? scheduler = null)
    {
        if (count <= 0)
        {
            return source;
        }

        return source.Operate<T, T>((src, subscriber) =>
        {
            var soFar = 0;
            var innerSubscription = new SingleAssignmentDisposable();

            void SubscribeForRetry()
            {
                innerSubscription.Disposable = src.Subscribe(
                    onNext: subscriber.OnNext,
                    onError: err =>
                    {
                        if (soFar++ < count)
                        {
                            if (delay is { } d)
                            {
                                var notifierSubscription = new SingleAssignmentDisposable();
                                notifierSubscription.Disposable = Observable.Timer(d, scheduler).Subscribe(
                                    onNext: _ =>
                                    {
                                        notifierSubscription.Dispose();
                                        SubscribeForRetry();
                                    });
                            }
                            else
                            {
                                SubscribeForRetry();
                            }
                        }
                        else
                        {
                            subscriber.OnError(err);
                        }
                    },
                    onComplete: subscriber.OnCompleted);
            }

            SubscribeForRetry();
            return innerSubscription;
        });
    }
}
