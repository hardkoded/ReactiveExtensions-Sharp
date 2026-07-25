namespace RxSharp.Operators;

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

            var sourceSubscription = src.Subscribe(
                onNext: value =>
                {
                    lastValue = value;
                    hasValue = true;
                },
                onError: subscriber.OnError,
                onComplete: subscriber.OnCompleted);
            subscriber.Add(sourceSubscription);

            var notifierSubscription = notifier.Subscribe(
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
            subscriber.Add(notifierSubscription);

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
            var timerSubscription = new SingleAssignmentDisposable();
            subscriber.Add(timerSubscription);
            var hasValue = false;
            T lastValue = default!;

            void Tick()
            {
                if (subscriber.IsDisposed)
                {
                    return;
                }

                if (hasValue)
                {
                    hasValue = false;
                    var value = lastValue;
                    lastValue = default!;
                    subscriber.OnNext(value);
                }

                timerSubscription.Disposable = activeScheduler.Schedule(Tick, period);
            }

            timerSubscription.Disposable = activeScheduler.Schedule(Tick, period);

            return src.Subscribe(
                onNext: value =>
                {
                    lastValue = value;
                    hasValue = true;
                },
                onError: subscriber.OnError,
                onComplete: subscriber.OnCompleted);
        });
}
