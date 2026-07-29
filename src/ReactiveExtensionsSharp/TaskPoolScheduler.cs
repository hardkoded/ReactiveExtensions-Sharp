namespace ReactiveExtensionsSharp;

/// <summary>The default <see cref="IScheduler"/>, backed by <see cref="Task.Delay(TimeSpan, CancellationToken)"/>.</summary>
public sealed class TaskPoolScheduler : IScheduler
{
    /// <summary>Gets the shared, stateless singleton instance of this scheduler.</summary>
    public static readonly TaskPoolScheduler Instance = new TaskPoolScheduler();

    /// <inheritdoc/>
    public DateTimeOffset Now => DateTimeOffset.UtcNow;

    /// <summary>
    /// Schedules <paramref name="action"/> via <see cref="Task.Delay(TimeSpan, CancellationToken)"/> on the default
    /// task scheduler. Disposing the returned subscription before <paramref name="dueTime"/> elapses cancels the
    /// delay so <paramref name="action"/> never runs.
    /// </summary>
    /// <param name="action">The work to run.</param>
    /// <param name="dueTime">How long to wait before running <paramref name="action"/>. A negative value is treated as zero.</param>
    /// <returns>A disposable that cancels the scheduled work if disposed before it runs.</returns>
    public IDisposable Schedule(Action action, TimeSpan dueTime)
    {
        var cts = new CancellationTokenSource();
        var due = dueTime < TimeSpan.Zero ? TimeSpan.Zero : dueTime;

        Task.Delay(due, cts.Token).ContinueWith(
            task =>
            {
                if (!task.IsCanceled)
                {
                    action();
                }
            },
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);

        return new Subscription(() =>
        {
            cts.Cancel();
            cts.Dispose();
        });
    }
}
