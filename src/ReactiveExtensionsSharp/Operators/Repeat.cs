namespace ReactiveExtensionsSharp.Operators;

/// <summary>Extension methods implementing the <c>repeat</c> operator.</summary>
/// <remarks>
/// rxjs's <c>repeat</c> accepts either a plain repeat count or a single <c>RepeatConfig</c> object combining
/// <c>count</c> and <c>delay</c> (a fixed number of milliseconds, or a per-cycle notifier-returning function).
/// As with <see cref="RetryOperator"/>, this port splits that one config object into two overloads instead of
/// introducing a config type: the original <see cref="Repeat{T}"/> (count + an optional fixed <see cref="TimeSpan"/>
/// delay), and <see cref="Repeat{T, TNotification}"/> (count + a per-cycle notifier-selector delay). rxjs's
/// <c>RepeatConfig</c> has no <c>resetOnSuccess</c> equivalent — that option only exists on <c>retry</c>, since
/// <c>repeat</c> has no error/success distinction to reset on.
/// </remarks>
public static class RepeatOperator
{
    /// <summary>
    /// Resubscribes to <paramref name="source"/> whenever it completes, up to <paramref name="count"/> times in
    /// total, instead of forwarding the completion downstream. Once the repeat budget is exhausted, completion
    /// is forwarded via <c>OnCompleted</c> as usual. Unlike <c>Retry</c>, an error from <paramref name="source"/>
    /// is never retried — it is forwarded via <c>OnError</c> immediately, ending the sequence.
    /// </summary>
    /// <remarks>
    /// This overload covers rxjs's <c>RepeatConfig</c> with a plain numeric or omitted <c>delay</c> (or none at
    /// all). For a per-cycle notifier-observable delay, use <see cref="Repeat{T, TNotification}"/> instead.
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

        return RepeatCore<T, long>(
            source,
            count,
            delay is { } fixedDelay ? _ => Observable.Timer(fixedDelay, scheduler) : null);
    }

    /// <summary>
    /// Resubscribes to <paramref name="source"/> whenever it completes, up to <paramref name="count"/> times in
    /// total, but waits for the observable returned by <paramref name="delaySelector"/> to emit before each
    /// resubscription instead of resubscribing immediately or after a fixed delay.
    /// </summary>
    /// <remarks>
    /// This is the function-based form of rxjs's <c>RepeatConfig.delay</c>. <paramref name="delaySelector"/> is
    /// called with the current 1-based cycle count (how many times <paramref name="source"/> has completed so
    /// far) and must return a notifier observable: its first emission triggers the resubscription, and an early
    /// completion (without ever emitting) completes the whole sequence instead of repeating. If <paramref name="delaySelector"/>
    /// itself throws, or the notifier it returns errors, that exception is forwarded via <c>OnError</c>.
    /// </remarks>
    /// <typeparam name="T">The type of values emitted by <paramref name="source"/>.</typeparam>
    /// <typeparam name="TNotification">The element type of the observable returned by <paramref name="delaySelector"/>; only its emissions/termination matter, not the values themselves.</typeparam>
    /// <param name="source">The source sequence to repeat on completion.</param>
    /// <param name="delaySelector">
    /// A function, called with the current 1-based cycle count, that returns an observable whose first emission
    /// triggers the resubscription.
    /// </param>
    /// <param name="count">The total number of times to subscribe to <paramref name="source"/>. Defaults to <see cref="int.MaxValue"/>.</param>
    /// <returns>An observable that mirrors <paramref name="source"/>, resubscribing on completion as described above.</returns>
    public static Observable<T> Repeat<T, TNotification>(this Observable<T> source, Func<int, Observable<TNotification>> delaySelector, int count = int.MaxValue)
    {
        if (count <= 0)
        {
            return Observable.Empty<T>();
        }

        return RepeatCore(source, count, delaySelector);
    }

    private static Observable<T> RepeatCore<T, TNotification>(Observable<T> source, int count, Func<int, Observable<TNotification>>? delaySelector)
        => source.Operate<T, T>((src, subscriber) =>
        {
            var soFar = 0;

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
                // contract. Registered as a child of `subscriber` before subscribing (and Remove()'d once the
                // cycle ends) for the same disposal-cascade reason as Retry's attemptSubscriber.
                Subscriber<T> cycleSubscriber = null!;
                cycleSubscriber = Subscriber.Create<T>(
                    onNext: subscriber.OnNext,
                    onError: subscriber.OnError,
                    onComplete: () =>
                    {
                        subscriber.Remove(cycleSubscriber);
                        cycleSubscriber.Dispose();

                        if (subscriber.IsDisposed)
                        {
                            return;
                        }

                        if (++soFar < count)
                        {
                            if (delaySelector is not null)
                            {
                                Observable<TNotification> notifier;
                                try
                                {
                                    notifier = delaySelector(soFar);
                                }
                                catch (Exception ex)
                                {
                                    subscriber.OnError(ex);
                                    return;
                                }

                                // Rebuilt fresh per cycle, so it must be Remove()'d once it fires — same
                                // reasoning as cycleSubscriber above, applied to the notifier instead.
                                Subscriber<TNotification> notifierSubscriber = null!;
                                notifierSubscriber = Subscriber.Create<TNotification>(
                                    onNext: _ =>
                                    {
                                        subscriber.Remove(notifierSubscriber);
                                        notifierSubscriber.Dispose();
                                        SubscribeForRepeat();
                                    },
                                    onError: subscriber.OnError,
                                    onComplete: subscriber.OnCompleted);
                                subscriber.Add(notifierSubscriber);
                                notifier.Subscribe(notifierSubscriber);
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

                subscriber.Add(cycleSubscriber);
                src.Subscribe(cycleSubscriber);
            }

            SubscribeForRepeat();
            return null;
        });
}
