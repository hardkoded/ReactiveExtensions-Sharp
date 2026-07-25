namespace RxSharp.Operators;

public static class ThrottleOperator
{
    /// <summary>
    /// Emits a value, then ignores subsequent values for a "duration" window opened by
    /// <paramref name="durationSelector"/>(that value). Matches rxjs's default config
    /// (<paramref name="leading"/>: <see langword="true"/>, <paramref name="trailing"/>: <see langword="false"/>):
    /// the leading edge of each window is emitted immediately and the trailing edge is dropped. Passing
    /// <paramref name="trailing"/>: <see langword="true"/> additionally emits the most recent value seen during the
    /// window once it closes. Mirrors rxjs's <c>throttle</c>.
    /// </summary>
    public static Observable<T> Throttle<T, TDuration>(
        this Observable<T> source,
        Func<T, Observable<TDuration>> durationSelector,
        bool leading = true,
        bool trailing = false)
        => source.Operate<T, T>((src, subscriber) =>
        {
            var throttled = new SingleAssignmentDisposable();
            subscriber.Add(throttled);
            var hasValue = false;
            var isThrottled = false;
            var isSourceComplete = false;
            T sendValue = default!;

            void Send()
            {
                if (!hasValue)
                {
                    return;
                }

                hasValue = false;
                var value = sendValue;
                sendValue = default!;
                subscriber.OnNext(value);
            }

            void StartThrottle(T value)
            {
                Observable<TDuration> duration;
                try
                {
                    duration = durationSelector(value);
                }
                catch (Exception ex)
                {
                    subscriber.OnError(ex);
                    return;
                }

                isThrottled = true;

                void ClearThrottle()
                {
                    isThrottled = false;
                    throttled.Disposable?.Dispose();
                    throttled.Disposable = null;
                    if (trailing)
                    {
                        Send();
                    }

                    if (isSourceComplete)
                    {
                        subscriber.OnCompleted();
                    }
                }

                throttled.Disposable = duration.Subscribe(
                    onNext: _ => ClearThrottle(),
                    onError: subscriber.OnError,
                    onComplete: ClearThrottle);
            }

            return src.Subscribe(
                onNext: value =>
                {
                    hasValue = true;
                    sendValue = value;

                    if (!isThrottled)
                    {
                        if (leading)
                        {
                            Send();
                        }

                        StartThrottle(value);
                    }
                },
                onError: subscriber.OnError,
                onComplete: () =>
                {
                    isSourceComplete = true;
                    if (!(trailing && hasValue && isThrottled))
                    {
                        subscriber.OnCompleted();
                    }
                });
        });

    /// <summary>
    /// <see cref="Throttle{T, TDuration}"/> with a fixed <see cref="TimeSpan"/> window instead of a per-value
    /// duration selector. Built on top of <see cref="Throttle{T, TDuration}"/> using <see cref="Observable.Timer"/> —
    /// safe to reuse (unlike <c>Delay</c>, see its own remarks) because only one duration subscription is ever
    /// active at a time, so there is no concurrent-timer ordering hazard.
    /// </summary>
    public static Observable<T> ThrottleTime<T>(
        this Observable<T> source,
        TimeSpan duration,
        IScheduler? scheduler = null,
        bool leading = true,
        bool trailing = false)
        => source.Throttle(_ => Observable.Timer(duration, scheduler), leading, trailing);
}
