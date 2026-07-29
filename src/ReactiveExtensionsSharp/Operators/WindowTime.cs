using ReactiveExtensionsSharp.Subjects;

namespace ReactiveExtensionsSharp.Operators;

/// <summary>The <c>windowTime</c> operator.</summary>
public static class WindowTimeOperator
{
    /// <summary>
    /// Like <see cref="WindowCountOperator.WindowCount{T}"/>, but windows open and close on a fixed time period
    /// instead of a value count. Mirrors rxjs's <c>windowTime</c>.
    /// </summary>
    /// <remarks>
    /// The first window opens eagerly at subscription time. When <paramref name="windowCreationInterval"/> is
    /// <see langword="null"/> (the two-argument form), a new window opens immediately whenever the previous one
    /// closes after <paramref name="windowTimeSpan"/> elapses, producing non-overlapping windows. When
    /// <paramref name="windowCreationInterval"/> is given, new windows instead open on their own independent
    /// timer (starting immediately, then every <paramref name="windowCreationInterval"/>), each closing
    /// <paramref name="windowTimeSpan"/> after it opened — so multiple windows can be open at once if
    /// <paramref name="windowCreationInterval"/> is shorter than <paramref name="windowTimeSpan"/>. Independent of
    /// either timer, a window also closes as soon as it has received <paramref name="maxWindowSize"/> values (a new
    /// one opens immediately in the non-overlapping, <paramref name="windowCreationInterval"/>-less form).
    /// </remarks>
    /// <typeparam name="T">The element type of the source.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="windowTimeSpan">How long each window stays open before closing.</param>
    /// <param name="windowCreationInterval">How often a new window opens. Defaults to <see langword="null"/> (open a new window right after the previous one closes).</param>
    /// <param name="maxWindowSize">The maximum number of values a window may collect before closing early, regardless of <paramref name="windowTimeSpan"/>. Defaults to unlimited.</param>
    /// <param name="scheduler">The scheduler to time windows with; defaults to <see cref="TaskPoolScheduler"/>.</param>
    /// <returns>An observable of windows, each itself an observable of the values collected during its lifetime.</returns>
    public static Observable<Observable<T>> WindowTime<T>(
        this Observable<T> source,
        TimeSpan windowTimeSpan,
        TimeSpan? windowCreationInterval = null,
        int? maxWindowSize = null,
        IScheduler? scheduler = null)
        => source.Operate<T, Observable<T>>((src, subscriber) =>
        {
            var activeScheduler = scheduler ?? TaskPoolScheduler.Instance;
            var maxSize = maxWindowSize ?? int.MaxValue;
            var records = new List<WindowRecord<T>>();
            var restartOnClose = windowCreationInterval is null;

            void CloseWindow(WindowRecord<T> record)
            {
                record.Window.OnCompleted();
                subscriber.Remove(record.CloseTimer);
                record.CloseTimer.Dispose();
                records.Remove(record);
                if (restartOnClose)
                {
                    StartWindow();
                }
            }

            void StartWindow()
            {
                var window = new Subject<T>();
                var record = new WindowRecord<T>(window);
                records.Add(record);
                subscriber.Add(record.CloseTimer);
                subscriber.OnNext(window.AsObservable());
                record.CloseTimer.Disposable = activeScheduler.Schedule(() => CloseWindow(record), windowTimeSpan);
            }

            if (windowCreationInterval is { } interval)
            {
                var creationTimer = new SingleAssignmentDisposable();
                subscriber.Add(creationTimer);

                void ScheduleNextCreation()
                {
                    creationTimer.Disposable = activeScheduler.Schedule(
                        () =>
                        {
                            StartWindow();
                            ScheduleNextCreation();
                        },
                        interval);
                }

                ScheduleNextCreation();
            }

            StartWindow();

            return src.SubscribeChild(
                subscriber,
                onNext: value =>
                {
                    foreach (var record in records.ToArray())
                    {
                        record.Window.OnNext(value);
                        record.Seen++;
                        if (record.Seen >= maxSize)
                        {
                            CloseWindow(record);
                        }
                    }
                },
                onError: error =>
                {
                    foreach (var record in records.ToArray())
                    {
                        record.Window.OnError(error);
                    }

                    records.Clear();
                    subscriber.OnError(error);
                },
                onComplete: () =>
                {
                    foreach (var record in records.ToArray())
                    {
                        record.Window.OnCompleted();
                    }

                    records.Clear();
                    subscriber.OnCompleted();
                });
        });

    private sealed class WindowRecord<T>
    {
        public WindowRecord(Subject<T> window)
        {
            Window = window;
            CloseTimer = new SingleAssignmentDisposable();
        }

        public Subject<T> Window { get; }

        public SingleAssignmentDisposable CloseTimer { get; }

        public int Seen { get; set; }
    }
}
