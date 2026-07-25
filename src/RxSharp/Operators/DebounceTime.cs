namespace RxSharp.Operators;

public static class DebounceTimeOperator
{
    /// <summary>
    /// Emits the most recent value only after <paramref name="dueTime"/> has passed without another emission.
    /// Simpler than rxjs's own implementation (which re-checks elapsed wall-clock time to avoid an unnecessary
    /// reschedule): here every new value just cancels and restarts the pending timer, which is functionally
    /// equivalent for a scheduler backed by real timers.
    /// </summary>
    public static Observable<T> DebounceTime<T>(this Observable<T> source, TimeSpan dueTime, IScheduler? scheduler = null)
        => source.Operate<T, T>((src, subscriber) =>
        {
            var activeScheduler = scheduler ?? TaskPoolScheduler.Instance;
            var activeTask = new SingleAssignmentDisposable();
            subscriber.Add(activeTask);
            var hasValue = false;
            T lastValue = default!;

            void Emit()
            {
                activeTask.Disposable?.Dispose();
                activeTask.Disposable = null;
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
                    activeTask.Disposable?.Dispose();
                    activeTask.Disposable = activeScheduler.Schedule(Emit, dueTime);
                },
                onError: subscriber.OnError,
                onComplete: () =>
                {
                    Emit();
                    subscriber.OnCompleted();
                });
        });
}
