namespace RxSharp.Operators;

/// <summary>Implements the <c>DebounceTime</c> operator. Mirrors rxjs's <c>debounceTime</c>.</summary>
public static class DebounceTimeOperator
{
    /// <summary>
    /// Emits the most recent value from <paramref name="source"/> only after <paramref name="dueTime"/> has passed
    /// without a further emission, dropping every value superseded within that quiet period.
    /// </summary>
    /// <remarks>
    /// Simpler than rxjs's own implementation (which re-checks elapsed wall-clock time to avoid an unnecessary
    /// reschedule): here every new value just cancels and restarts the pending timer, which is functionally
    /// equivalent for a scheduler backed by real timers. If <paramref name="source"/> completes while a value is
    /// still pending, that value is emitted immediately before completion is forwarded. If <paramref name="source"/>
    /// errors, the pending value is discarded and the error is forwarded immediately, without waiting out the
    /// remainder of <paramref name="dueTime"/>.
    /// </remarks>
    /// <typeparam name="T">The type of the values being debounced.</typeparam>
    /// <param name="source">The source observable to debounce.</param>
    /// <param name="dueTime">The quiet period required, after the most recent value, before it is let through.</param>
    /// <param name="scheduler">
    /// The scheduler used to run the debounce timer. Defaults to <see cref="TaskPoolScheduler.Instance"/> when
    /// <see langword="null"/>.
    /// </param>
    /// <returns>
    /// An observable that emits each value from <paramref name="source"/> only after <paramref name="dueTime"/>
    /// has elapsed without a newer value arriving.
    /// </returns>
    public static Observable<T> DebounceTime<T>(this Observable<T> source, TimeSpan dueTime, IScheduler? scheduler = null)
        => source.Operate<T, T>((src, subscriber) =>
        {
            var activeScheduler = scheduler ?? TaskPoolScheduler.Instance;
            IDisposable? activeTask = null;
            var hasValue = false;
            T lastValue = default!;

            // Cancels and forgets the currently-pending scheduled callback, if any. Removing it from `subscriber`'s
            // finalizer list (not just disposing it) keeps that list bounded across a long-running stream instead
            // of growing by one stale entry per source value. `IScheduler.Schedule` on `TaskPoolScheduler` always
            // defers via `Task.Delay`, so there is no reentrancy hazard here the way there is for a duration
            // observable that can complete synchronously (see `Debounce`/`Audit`/`Throttle`).
            void ClearTask()
            {
                if (activeTask is null)
                {
                    return;
                }

                subscriber.Remove(activeTask);
                activeTask.Dispose();
                activeTask = null;
            }

            void Emit()
            {
                ClearTask();
                if (!hasValue)
                {
                    return;
                }

                hasValue = false;
                var value = lastValue;
                lastValue = default!;
                subscriber.OnNext(value);
            }

            return src.SubscribeChild(
                subscriber,
                onNext: value =>
                {
                    lastValue = value;
                    hasValue = true;
                    ClearTask();
                    var scheduled = activeScheduler.Schedule(Emit, dueTime);
                    activeTask = scheduled;
                    subscriber.Add(scheduled);
                },
                onError: subscriber.OnError,
                onComplete: () =>
                {
                    Emit();
                    subscriber.OnCompleted();
                });
        });
}
