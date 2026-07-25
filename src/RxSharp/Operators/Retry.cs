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

            void SubscribeForRetry()
            {
                // Guards against recursing into another attempt once downstream has already gone away — same
                // stack-overflow-avoidance reasoning as Repeat/RetryWhen (see CLAUDE.md): a rapidly,
                // synchronously erroring source with a large retry count would otherwise keep recursing forever
                // even after downstream stopped caring.
                if (subscriber.IsDisposed)
                {
                    return;
                }

                // Built directly (see OperatorHelper.SubscribeChild's doc comment for the general pattern) and
                // registered as a child of `subscriber` *before* subscribing, rather than via the
                // Subscribe(onNext:...) convenience overload. This lets a downstream disposal (e.g. an
                // early-completing operator further down the chain) cascade up and stop a fully-synchronous
                // source mid-attempt, not just once the whole synchronous call stack unwinds. Unlike the
                // single-stable-inner-subscription operators SubscribeChild targets, a new attemptSubscriber
                // replaces the previous one on every retry, so it must also be Remove()'d once the attempt ends
                // (error handled or completed) to avoid the downstream subscriber's finalizer list growing
                // unboundedly across many retries.
                Subscriber<T> attemptSubscriber = null!;
                attemptSubscriber = Subscriber.Create<T>(
                    onNext: subscriber.OnNext,
                    onError: err =>
                    {
                        subscriber.Remove(attemptSubscriber);
                        attemptSubscriber.Dispose();

                        if (subscriber.IsDisposed)
                        {
                            return;
                        }

                        if (soFar++ < count)
                        {
                            if (delay is { } d)
                            {
                                Subscriber<long> timerSubscriber = null!;
                                timerSubscriber = Subscriber.Create<long>(onNext: _ =>
                                {
                                    subscriber.Remove(timerSubscriber);
                                    timerSubscriber.Dispose();
                                    SubscribeForRetry();
                                });
                                subscriber.Add(timerSubscriber);
                                Observable.Timer(d, scheduler).Subscribe(timerSubscriber);
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
                    onComplete: () =>
                    {
                        subscriber.Remove(attemptSubscriber);
                        attemptSubscriber.Dispose();
                        subscriber.OnCompleted();
                    });

                subscriber.Add(attemptSubscriber);
                src.Subscribe(attemptSubscriber);
            }

            SubscribeForRetry();
            return null;
        });
    }
}
