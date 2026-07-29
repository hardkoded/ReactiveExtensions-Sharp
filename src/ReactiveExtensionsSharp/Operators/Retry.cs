namespace ReactiveExtensionsSharp.Operators;

/// <summary>Extension methods implementing the <c>retry</c> operator.</summary>
/// <remarks>
/// rxjs's <c>retry</c> accepts either a plain retry count or a single <c>RetryConfig</c> object combining
/// <c>count</c>, <c>delay</c> (a fixed number of milliseconds, or a per-error notifier-returning function), and
/// <c>resetOnSuccess</c>. C# has no object-literal equivalent that reads as naturally as rxjs's, so this port
/// splits that one config object into two overloads instead: the original <see cref="Retry{T}"/> (count + an
/// optional fixed <see cref="TimeSpan"/> delay + <c>resetOnSuccess</c>), and <see cref="Retry{T, TNotification}"/>
/// (count + a per-error notifier-selector delay + <c>resetOnSuccess</c>) for the function-based delay form. Both
/// funnel into the same private <see cref="RetryCore{T, TNotification}"/> implementation.
/// </remarks>
public static class RetryOperator
{
    /// <summary>
    /// Resubscribes to <paramref name="source"/> whenever it errors, up to <paramref name="count"/> times,
    /// instead of forwarding the error downstream. Once the retry budget is exhausted, the most recent error
    /// is forwarded via <c>OnError</c> as usual. Values emitted before a failed attempt are still forwarded
    /// downstream; a retry only affects what happens after the error.
    /// </summary>
    /// <remarks>
    /// This overload covers rxjs's <c>RetryConfig</c> with a plain numeric or omitted <c>delay</c> (or none at
    /// all). For a per-error notifier-observable delay, use <see cref="Retry{T, TNotification}"/> instead.
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
    /// <param name="resetOnSuccess">
    /// If <see langword="true"/>, the retry counter resets to zero the moment <paramref name="source"/> emits
    /// its first value after a resubscription — so a long-running stream that errors only intermittently never
    /// exhausts its retry budget from historical errors. Defaults to <see langword="false"/>, matching rxjs.
    /// </param>
    /// <returns>An observable that mirrors <paramref name="source"/>, retrying on error as described above.</returns>
    public static Observable<T> Retry<T>(this Observable<T> source, int count = int.MaxValue, TimeSpan? delay = null, IScheduler? scheduler = null, bool resetOnSuccess = false)
    {
        if (count <= 0)
        {
            return source;
        }

        return RetryCore<T, long>(
            source,
            count,
            resetOnSuccess,
            delay is { } fixedDelay ? (_, _) => Observable.Timer(fixedDelay, scheduler) : null);
    }

    /// <summary>
    /// Resubscribes to <paramref name="source"/> whenever it errors, up to <paramref name="count"/> times, but
    /// waits for the observable returned by <paramref name="delaySelector"/> to emit before each resubscription
    /// instead of resubscribing immediately or after a fixed delay.
    /// </summary>
    /// <remarks>
    /// This is the function-based form of rxjs's <c>RetryConfig.delay</c>. <paramref name="delaySelector"/> is
    /// called with the error that just occurred and the current 1-based retry attempt number, and must return a
    /// notifier observable: its first emission triggers the resubscription, an early completion (without ever
    /// emitting) completes the whole sequence instead of retrying, and an error is forwarded downstream via
    /// <c>OnError</c> in place of the original error. If <paramref name="delaySelector"/> itself throws, that
    /// exception is forwarded via <c>OnError</c> too.
    /// </remarks>
    /// <typeparam name="T">The type of values emitted by <paramref name="source"/>.</typeparam>
    /// <typeparam name="TNotification">The element type of the observable returned by <paramref name="delaySelector"/>; only its emissions/termination matter, not the values themselves.</typeparam>
    /// <param name="source">The source sequence to retry on error.</param>
    /// <param name="delaySelector">
    /// A function, called with the error that just occurred and the current 1-based retry attempt number, that
    /// returns an observable whose first emission triggers the resubscription.
    /// </param>
    /// <param name="count">The maximum number of resubscriptions to attempt after an error. Defaults to <see cref="int.MaxValue"/>.</param>
    /// <param name="resetOnSuccess">
    /// If <see langword="true"/>, the retry counter resets to zero the moment <paramref name="source"/> emits
    /// its first value after a resubscription. Defaults to <see langword="false"/>, matching rxjs.
    /// </param>
    /// <returns>An observable that mirrors <paramref name="source"/>, retrying on error as described above.</returns>
    public static Observable<T> Retry<T, TNotification>(this Observable<T> source, Func<Exception, int, Observable<TNotification>> delaySelector, int count = int.MaxValue, bool resetOnSuccess = false)
    {
        if (count <= 0)
        {
            return source;
        }

        return RetryCore(source, count, resetOnSuccess, delaySelector);
    }

    private static Observable<T> RetryCore<T, TNotification>(
        Observable<T> source,
        int count,
        bool resetOnSuccess,
        Func<Exception, int, Observable<TNotification>>? delaySelector)
        => source.Operate<T, T>((src, subscriber) =>
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
                    onNext: value =>
                    {
                        if (resetOnSuccess)
                        {
                            soFar = 0;
                        }

                        subscriber.OnNext(value);
                    },
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
                            if (delaySelector is not null)
                            {
                                Observable<TNotification> notifier;
                                try
                                {
                                    notifier = delaySelector(err, soFar);
                                }
                                catch (Exception ex)
                                {
                                    subscriber.OnError(ex);
                                    return;
                                }

                                // Rebuilt fresh per attempt (unlike SubscribeChild's single-lifetime target), so
                                // it must be Remove()'d once it fires — see attemptSubscriber's own comment above
                                // for the same reasoning applied to the notifier instead of the source attempt.
                                Subscriber<TNotification> notifierSubscriber = null!;
                                notifierSubscriber = Subscriber.Create<TNotification>(
                                    onNext: _ =>
                                    {
                                        subscriber.Remove(notifierSubscriber);
                                        notifierSubscriber.Dispose();
                                        SubscribeForRetry();
                                    },
                                    onError: subscriber.OnError,
                                    onComplete: subscriber.OnCompleted);
                                subscriber.Add(notifierSubscriber);
                                notifier.Subscribe(notifierSubscriber);
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
