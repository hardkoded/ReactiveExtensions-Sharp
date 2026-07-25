namespace RxSharp;

/// <summary>The default <see cref="IScheduler"/>, backed by <see cref="Task.Delay(TimeSpan, CancellationToken)"/>.</summary>
public sealed class TaskPoolScheduler : IScheduler
{
    public static readonly TaskPoolScheduler Instance = new TaskPoolScheduler();

    public DateTimeOffset Now => DateTimeOffset.UtcNow;

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
