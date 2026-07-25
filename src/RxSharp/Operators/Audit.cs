namespace RxSharp.Operators;

/// <summary>The <c>audit</c>/<c>auditTime</c> operators.</summary>
public static class AuditOperator
{
    /// <summary>
    /// Emits the most recent source value once a "duration" window elapses, ignoring values that arrive while a
    /// window is already open (the inverse of <c>debounce</c>, which restarts its timer on every new value instead
    /// of ignoring values mid-window). A window opens on the first value seen while idle, via
    /// <paramref name="durationSelector"/>(that value); the window closes — and the latest pending value is emitted
    /// — as soon as the duration observable emits or completes. Mirrors rxjs's <c>audit</c>.
    /// </summary>
    /// <typeparam name="T">The element type of the source.</typeparam>
    /// <typeparam name="TDuration">The (unused) element type of the duration observable.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="durationSelector">Given the value that opened the current window, returns the observable whose first emission or completion closes it.</param>
    /// <returns>An observable that emits the most recent value once each window closes.</returns>
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
    /// <typeparam name="T">The element type of the source.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="duration">The fixed window length.</param>
    /// <param name="scheduler">The scheduler to time the window with; defaults to <see cref="TaskPoolScheduler"/>.</param>
    /// <returns>An observable that emits the most recent value once each window closes.</returns>
    public static Observable<T> AuditTime<T>(this Observable<T> source, TimeSpan duration, IScheduler? scheduler = null)
        => source.Audit(_ => Observable.Timer(duration, scheduler));
}
