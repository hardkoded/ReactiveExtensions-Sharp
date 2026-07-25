namespace RxSharp.Operators;

/// <summary>Implements the <c>Delay</c> operator. Mirrors rxjs's <c>delay</c>.</summary>
public static class DelayOperator
{
    /// <summary>
    /// Time-shifts every value from <paramref name="source"/> by <paramref name="due"/>, preserving relative order.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Deliberately NOT built on <c>DelayWhen</c>/<c>MergeMap</c>: those subscribe to one independent timer per
    /// value concurrently, and real timers (backed by the thread pool) don't guarantee same-duration timers
    /// fire back in the order they were scheduled — which silently breaks the ordering guarantee this operator's
    /// own documentation (and rxjs's) promises. Instead, this serializes emission through a single queue: only
    /// one timer is ever active, always for the queue head, so firing order always matches arrival order
    /// regardless of scheduler jitter.
    /// </para>
    /// <para>
    /// Known simplification: for a <i>burst</i> of synchronous values, each one's delay is measured from when
    /// its predecessor fired rather than from its own arrival, so latencies stack (item N fires at roughly
    /// N &#215; <paramref name="due"/>) instead of each value landing at a uniform offset from when it arrived.
    /// Correct for the common "space out slower-than-due emissions" case; revisit if a real use case needs
    /// precise per-item latency under bursts (that's what the full scheduler/timer-queue work in M4 is for).
    /// </para>
    /// <para>
    /// Errors are not queued: an error from <paramref name="source"/> is forwarded immediately, without waiting
    /// for any already-queued values to be flushed.
    /// </para>
    /// </remarks>
    /// <typeparam name="T">The type of the values being delayed.</typeparam>
    /// <param name="source">The source observable to delay.</param>
    /// <param name="due">The amount of time by which to shift each value (see remarks for burst behavior).</param>
    /// <param name="scheduler">
    /// The scheduler used to run the delay timer. Defaults to <see cref="TaskPoolScheduler.Instance"/> when
    /// <see langword="null"/>.
    /// </param>
    /// <returns>
    /// An observable that emits the same values as <paramref name="source"/>, each shifted later in time by
    /// roughly <paramref name="due"/>, followed by the same completion or (immediate) error notification.
    /// </returns>
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
