namespace RxSharp.Operators;

/// <summary>Extension methods implementing the <c>repeat</c> operator.</summary>
public static class RepeatOperator
{
    /// <summary>
    /// Resubscribes to <paramref name="source"/> whenever it completes, up to <paramref name="count"/> times in
    /// total, instead of forwarding the completion downstream. Once the repeat budget is exhausted, completion
    /// is forwarded via <c>OnCompleted</c> as usual. Unlike <c>Retry</c>, an error from <paramref name="source"/>
    /// is never retried — it is forwarded via <c>OnError</c> immediately, ending the sequence.
    /// </summary>
    /// <remarks>
    /// This is a reduced form of rxjs's <c>repeat</c>: it supports a repeat count and, optionally, a single
    /// fixed delay before each resubscription (via a <c>Timer</c> on <paramref name="scheduler"/>), mirroring
    /// <see cref="RetryOperator"/>'s structure, but not rxjs's <c>delay</c>-as-a-per-count-notifier-function
    /// form.
    /// </remarks>
    /// <typeparam name="T">The type of values emitted by <paramref name="source"/>.</typeparam>
    /// <param name="source">The source sequence to repeat on completion.</param>
    /// <param name="count">
    /// The total number of times to subscribe to <paramref name="source"/>. Defaults to <see cref="int.MaxValue"/>
    /// (effectively repeating forever). A value of zero or less returns an already-completed, empty observable
    /// without ever subscribing to <paramref name="source"/> — unlike <see cref="RetryOperator.Retry{T}"/>'s
    /// zero-or-less handling, which still subscribes once.
    /// </param>
    /// <param name="delay">
    /// An optional fixed delay to wait before each resubscription. If <see langword="null"/> (the default),
    /// resubscription happens immediately after completion.
    /// </param>
    /// <param name="scheduler">The scheduler used to time <paramref name="delay"/>, if provided.</param>
    /// <returns>An observable that mirrors <paramref name="source"/>, resubscribing on completion as described above.</returns>
    public static Observable<T> Repeat<T>(this Observable<T> source, int count = int.MaxValue, TimeSpan? delay = null, IScheduler? scheduler = null)
    {
        if (count <= 0)
        {
            return Observable.Empty<T>();
        }

        return source.Operate<T, T>((src, subscriber) =>
        {
            var soFar = 0;
            var sourceSubscription = new SingleAssignmentDisposable();

            void SubscribeForRepeat()
            {
                // Guards against recursing into another cycle once downstream has already gone away — e.g. a
                // fully synchronous source paired with a downstream Take(n): each cycle's own completion is
                // independent of the shared downstream subscriber's state (a fresh per-cycle subscriber is
                // created on every call to Subscribe), so without this check a source that keeps completing
                // synchronously would keep recursing into new cycles forever (eventually a
                // StackOverflowException) even after downstream stopped caring.
                if (subscriber.IsDisposed)
                {
                    return;
                }

                // Built directly (see Take.cs for the full explanation) rather than via the Subscribe(onNext:...)
                // convenience overload, so the completion callback can dispose *this exact, already-live*
                // per-cycle subscriber itself before recursing into the next cycle. That matters because
                // completion, for a fully synchronous source, dispatches to this callback *before* Subscribe
                // ever returns — so any teardown the source registered on this subscriber (see Observable's
                // Action<Subscriber<T>> constructor) needs to run here, not after the whole recursive chain of
                // future cycles has already unwound, to match rxjs's "always finalize before the next cycle"
                // contract.
                Subscriber<T> cycleSubscriber = null!;
                cycleSubscriber = Subscriber.Create<T>(
                    onNext: subscriber.OnNext,
                    onError: subscriber.OnError,
                    onComplete: () =>
                    {
                        cycleSubscriber.Dispose();

                        if (subscriber.IsDisposed)
                        {
                            return;
                        }

                        if (++soFar < count)
                        {
                            if (delay is { } d)
                            {
                                var notifierSubscription = new SingleAssignmentDisposable();
                                notifierSubscription.Disposable = Observable.Timer(d, scheduler).Subscribe(
                                    onNext: _ =>
                                    {
                                        notifierSubscription.Dispose();
                                        SubscribeForRepeat();
                                    });
                            }
                            else
                            {
                                SubscribeForRepeat();
                            }
                        }
                        else
                        {
                            subscriber.OnCompleted();
                        }
                    });

                sourceSubscription.Disposable = src.Subscribe(cycleSubscriber);
            }

            SubscribeForRepeat();
            return sourceSubscription;
        });
    }
}
