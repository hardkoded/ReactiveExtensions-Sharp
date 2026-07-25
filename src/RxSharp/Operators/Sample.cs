namespace RxSharp.Operators;

public static class SampleOperator
{
    /// <summary>
    /// Emits the most recent source value whenever <paramref name="notifier"/> emits. A notifier tick with no new
    /// source value since the last sample produces no emission. The notifier completing does not complete (or
    /// otherwise affect) the result — sampling simply stops happening once it does. Mirrors rxjs's <c>sample</c>.
    /// </summary>
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
