namespace RxSharp.Operators;

/// <summary>The <c>bufferTime</c> operator.</summary>
public static class BufferTimeOperator
{
    /// <summary>
    /// Like <see cref="BufferCountOperator.BufferCount{T}"/>, but buffers close on a fixed time period instead of a
    /// value count. Mirrors rxjs's <c>bufferTime</c>. Implemented directly against <paramref name="source"/> (the
    /// same open-buffers-list algorithm as <see cref="WindowTimeOperator.WindowTime{T}"/>, but collecting into
    /// <see cref="List{T}"/> buffers instead of emitting live windows) rather than composing
    /// <c>WindowTime</c> + a per-window collector, to avoid a dependency on an unrelated, not-yet-existing operator.
    /// </summary>
    /// <remarks>
    /// The first buffer opens eagerly at subscription time. When <paramref name="bufferCreationInterval"/> is
    /// <see langword="null"/> (the two-argument form), a new buffer opens immediately whenever the previous one
    /// closes and is emitted after <paramref name="bufferTimeSpan"/> elapses, producing non-overlapping buffers.
    /// When <paramref name="bufferCreationInterval"/> is given, new buffers instead open on their own independent
    /// timer (starting immediately, then every <paramref name="bufferCreationInterval"/>), each emitted
    /// <paramref name="bufferTimeSpan"/> after it opened — so multiple buffers can be collecting at once if
    /// <paramref name="bufferCreationInterval"/> is shorter than <paramref name="bufferTimeSpan"/>. Independent of
    /// either timer, a buffer also closes and emits as soon as it has collected <paramref name="maxBufferSize"/>
    /// values (a new one opens immediately in the non-overlapping, <paramref name="bufferCreationInterval"/>-less form).
    /// If <paramref name="source"/> completes while one or more buffers are still open, those partial buffers are
    /// emitted as-is, in the order they were opened, before completion is forwarded.
    /// </remarks>
    /// <typeparam name="T">The type of the elements collected into each buffer.</typeparam>
    /// <param name="source">The source observable to buffer.</param>
    /// <param name="bufferTimeSpan">How long each buffer stays open before being emitted.</param>
    /// <param name="bufferCreationInterval">How often a new buffer opens. Defaults to <see langword="null"/> (open a new buffer right after the previous one closes).</param>
    /// <param name="maxBufferSize">The maximum number of values a buffer may collect before closing early, regardless of <paramref name="bufferTimeSpan"/>. Defaults to unlimited.</param>
    /// <param name="scheduler">The scheduler to time buffers with; defaults to <see cref="TaskPoolScheduler"/>.</param>
    /// <returns>An observable of each completed (or trailing partial) buffer as an <see cref="IReadOnlyList{T}"/>.</returns>
    public static Observable<IReadOnlyList<T>> BufferTime<T>(
        this Observable<T> source,
        TimeSpan bufferTimeSpan,
        TimeSpan? bufferCreationInterval = null,
        int? maxBufferSize = null,
        IScheduler? scheduler = null)
        => source.Operate<T, IReadOnlyList<T>>((src, subscriber) =>
        {
            var activeScheduler = scheduler ?? TaskPoolScheduler.Instance;
            var maxSize = maxBufferSize ?? int.MaxValue;
            var records = new List<BufferRecord<T>>();
            var restartOnClose = bufferCreationInterval is null;

            void CloseBuffer(BufferRecord<T> record)
            {
                subscriber.OnNext(record.Values);
                subscriber.Remove(record.CloseTimer);
                record.CloseTimer.Dispose();
                records.Remove(record);
                if (restartOnClose)
                {
                    StartBuffer();
                }
            }

            void StartBuffer()
            {
                var record = new BufferRecord<T>();
                records.Add(record);
                subscriber.Add(record.CloseTimer);
                record.CloseTimer.Disposable = activeScheduler.Schedule(() => CloseBuffer(record), bufferTimeSpan);
            }

            if (bufferCreationInterval is { } interval)
            {
                var creationTimer = new SingleAssignmentDisposable();
                subscriber.Add(creationTimer);

                void ScheduleNextCreation()
                {
                    creationTimer.Disposable = activeScheduler.Schedule(
                        () =>
                        {
                            StartBuffer();
                            ScheduleNextCreation();
                        },
                        interval);
                }

                ScheduleNextCreation();
            }

            StartBuffer();

            return src.SubscribeChild(
                subscriber,
                onNext: value =>
                {
                    foreach (var record in records.ToArray())
                    {
                        record.Values.Add(value);
                        if (record.Values.Count >= maxSize)
                        {
                            CloseBuffer(record);
                        }
                    }
                },
                onError: error =>
                {
                    records.Clear();
                    subscriber.OnError(error);
                },
                onComplete: () =>
                {
                    foreach (var record in records.ToArray())
                    {
                        subscriber.OnNext(record.Values);
                    }

                    records.Clear();
                    subscriber.OnCompleted();
                });
        });

    private sealed class BufferRecord<T>
    {
        public List<T> Values { get; } = new List<T>();

        public SingleAssignmentDisposable CloseTimer { get; } = new SingleAssignmentDisposable();
    }
}
