namespace RxSharp.Operators;

public static class DelayOperator
{
    /// <summary>
    /// Time-shifts every value by <paramref name="due"/>, preserving relative order. Deliberately NOT
    /// built on <c>DelayWhen</c>/<c>MergeMap</c>: those subscribe to one independent timer per value
    /// concurrently, and real timers (backed by the thread pool) don't guarantee same-duration timers
    /// fire back in the order they were scheduled — which silently breaks the ordering guarantee this
    /// operator's own documentation (and rxjs's) promises. Instead, this serializes emission through a
    /// single queue: only one timer is ever active, always for the queue head, so firing order always
    /// matches arrival order regardless of scheduler jitter.
    /// Known simplification: for a *burst* of synchronous values, each one's delay is measured from when
    /// its predecessor fired rather than from its own arrival, so latencies stack (item N fires at
    /// roughly N × <paramref name="due"/>) instead of each value landing at a uniform offset from when it
    /// arrived. Correct for the common "space out slower-than-due emissions" case; revisit if a real use
    /// case needs precise per-item latency under bursts (that's what the full scheduler/timer-queue work
    /// in M4 is for).
    /// </summary>
    public static Observable<T> Delay<T>(this Observable<T> source, TimeSpan due, IScheduler? scheduler = null)
        => source.Operate<T, T>((src, subscriber) =>
        {
            var activeScheduler = scheduler ?? TaskPoolScheduler.Instance;
            var queue = new Queue<T>();
            var isSourceComplete = false;
            var activeTimer = new SingleAssignmentDisposable();
            subscriber.Add(activeTimer);

            void ScheduleNext()
            {
                if (queue.Count == 0)
                {
                    if (isSourceComplete)
                    {
                        subscriber.OnCompleted();
                    }

                    return;
                }

                activeTimer.Disposable = activeScheduler.Schedule(
                    () =>
                    {
                        var value = queue.Dequeue();
                        subscriber.OnNext(value);
                        ScheduleNext();
                    },
                    due);
            }

            return src.Subscribe(
                onNext: value =>
                {
                    var wasEmpty = queue.Count == 0;
                    queue.Enqueue(value);
                    if (wasEmpty)
                    {
                        ScheduleNext();
                    }
                },
                onError: subscriber.OnError,
                onComplete: () =>
                {
                    isSourceComplete = true;
                    if (queue.Count == 0)
                    {
                        subscriber.OnCompleted();
                    }
                });
        });
}
