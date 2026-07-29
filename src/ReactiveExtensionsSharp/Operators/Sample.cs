namespace ReactiveExtensionsSharp.Operators;

/// <summary>The <c>sample</c>/<c>sampleTime</c> operators.</summary>
public static class SampleOperator
{
    /// <summary>
    /// Emits the most recent source value whenever <paramref name="notifier"/> emits. A notifier tick with no new
    /// source value since the last sample produces no emission. The notifier completing does not complete (or
    /// otherwise affect) the result — sampling simply stops happening once it does. Mirrors rxjs's <c>sample</c>.
    /// </summary>
    /// <typeparam name="T">The element type of the source.</typeparam>
    /// <typeparam name="TNotifier">The (unused) element type of the notifier.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="notifier">Emits to trigger each sample.</param>
    /// <returns>An observable that emits the most recent source value on every notifier tick.</returns>
    public static Observable<T> Sample<T, TNotifier>(this Observable<T> source, Observable<TNotifier> notifier)
        => source.Operate<T, T>((src, subscriber) =>
        {
            var hasValue = false;
            T lastValue = default!;

            // Both the source and the notifier are subscribed exactly once for the whole lifetime of this
            // operator (unlike Debounce/Audit/Throttle, there is no per-value/per-cycle resubscription here), so
            // each is eligible for the simple SubscribeChild helper directly.
            src.SubscribeChild(
                subscriber,
                onNext: value =>
                {
                    lastValue = value;
                    hasValue = true;
                },
                onError: subscriber.OnError,
                onComplete: subscriber.OnCompleted);

            notifier.SubscribeChild(
                subscriber,
                onNext: _ =>
                {
                    if (!hasValue)
                    {
                        return;
                    }

                    hasValue = false;
                    var value = lastValue;
                    lastValue = default!;
                    subscriber.OnNext(value);
                },
                onError: subscriber.OnError);

            return null;
        });

    /// <summary>
    /// <see cref="Sample{T, TNotifier}"/> sampled by a fixed <paramref name="period"/> instead of a notifier
    /// observable. Implemented as a self-rescheduling timer (there is no <c>Observable.Interval</c> creation
    /// function yet) rather than layering on <see cref="Sample{T, TNotifier}"/>.
    /// </summary>
    /// <typeparam name="T">The element type of the source.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="period">The fixed sampling period.</param>
    /// <param name="scheduler">The scheduler to time the period with; defaults to <see cref="TaskPoolScheduler"/>.</param>
    /// <returns>An observable that emits the most recent source value on every tick of the period.</returns>
    public static Observable<T> SampleTime<T>(this Observable<T> source, TimeSpan period, IScheduler? scheduler = null)
        => source.Operate<T, T>((src, subscriber) =>
        {
            var activeScheduler = scheduler ?? TaskPoolScheduler.Instance;
            IDisposable? timerSubscription = null;
            var hasValue = false;
            T lastValue = default!;

            // Cancels and forgets the currently-pending tick, if any. Removing it from `subscriber`'s finalizer
            // list (not just disposing it) keeps that list bounded across a long-running stream instead of
            // growing by one stale entry per tick. `IScheduler.Schedule` on `TaskPoolScheduler` always defers via
            // `Task.Delay`, so there is no reentrancy hazard here the way there is for a duration observable that
            // can complete synchronously (see `Debounce`/`Audit`/`Throttle`).
            void ClearTimer()
            {
                if (timerSubscription is null)
                {
                    return;
                }

                subscriber.Remove(timerSubscription);
                timerSubscription.Dispose();
                timerSubscription = null;
            }

            void ScheduleNextTick()
            {
                var scheduled = activeScheduler.Schedule(Tick, period);
                timerSubscription = scheduled;
                subscriber.Add(scheduled);
            }

            void Tick()
            {
                if (subscriber.IsDisposed)
                {
                    return;
                }

                ClearTimer();

                if (hasValue)
                {
                    hasValue = false;
                    var value = lastValue;
                    lastValue = default!;
                    subscriber.OnNext(value);
                }

                ScheduleNextTick();
            }

            ScheduleNextTick();

            return src.SubscribeChild(
                subscriber,
                onNext: value =>
                {
                    lastValue = value;
                    hasValue = true;
                },
                onError: subscriber.OnError,
                onComplete: subscriber.OnCompleted);
        });
}
