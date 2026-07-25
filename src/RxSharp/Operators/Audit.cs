namespace RxSharp.Operators;

public static class AuditOperator
{
    /// <summary>
    /// Emits the most recent source value once a "duration" window elapses, ignoring values that arrive while a
    /// window is already open (the inverse of <c>debounce</c>, which restarts its timer on every new value instead
    /// of ignoring values mid-window). A window opens on the first value seen while idle, via
    /// <paramref name="durationSelector"/>(that value); the window closes — and the latest pending value is emitted
    /// — as soon as the duration observable emits or completes. Mirrors rxjs's <c>audit</c>.
    /// </summary>
    public static Observable<T> Audit<T, TDuration>(this Observable<T> source, Func<T, Observable<TDuration>> durationSelector)
        => source.Operate<T, T>((src, subscriber) =>
        {
            var durationSubscription = new SingleAssignmentDisposable();
            subscriber.Add(durationSubscription);
            var hasValue = false;
            var isAuditing = false;
            T lastValue = default!;

            void CloseWindow()
            {
                durationSubscription.Disposable?.Dispose();
                durationSubscription.Disposable = null;
                isAuditing = false;
                if (!hasValue)
                {
                    return;
                }

                hasValue = false;
                var value = lastValue;
                lastValue = default!;
                subscriber.OnNext(value);
            }

            return src.Subscribe(
                onNext: value =>
                {
                    lastValue = value;
                    hasValue = true;

                    if (isAuditing)
                    {
                        return;
                    }

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

                    isAuditing = true;
                    durationSubscription.Disposable = duration.Subscribe(
                        onNext: _ => CloseWindow(),
                        onError: subscriber.OnError,
                        onComplete: CloseWindow);
                },
                onError: subscriber.OnError,
                onComplete: () =>
                {
                    CloseWindow();
                    subscriber.OnCompleted();
                });
        });

    /// <summary>
    /// <see cref="Audit{T, TDuration}"/> with a fixed <see cref="TimeSpan"/> window instead of a per-value duration
    /// selector. Built on top of <see cref="Audit{T, TDuration}"/> using <see cref="Observable.Timer"/> — safe to
    /// reuse (unlike <c>Delay</c>, see its own remarks) because <c>Audit</c> only ever has one duration subscription
    /// active at a time, so there is no concurrent-timer ordering hazard.
    /// </summary>
    public static Observable<T> AuditTime<T>(this Observable<T> source, TimeSpan duration, IScheduler? scheduler = null)
        => source.Audit(_ => Observable.Timer(duration, scheduler));
}
