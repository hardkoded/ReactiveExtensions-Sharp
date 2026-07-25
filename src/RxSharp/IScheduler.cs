namespace RxSharp;

/// <summary>Schedules work after a delay. Mirrors rxjs's <c>SchedulerLike</c>. A minimal seam for now — see CLAUDE.md for the plan to grow this into a full scheduler hierarchy with a virtual-time implementation.</summary>
public interface IScheduler
{
    /// <summary>Gets the scheduler's current notion of "now". Used by time-based operators instead of reading the system clock directly, so a future virtual-time scheduler can control it.</summary>
    DateTimeOffset Now { get; }

    /// <summary>Schedules <paramref name="action"/> to run after <paramref name="dueTime"/> elapses.</summary>
    /// <param name="action">The work to run.</param>
    /// <param name="dueTime">How long to wait before running <paramref name="action"/>. A negative value is treated as zero.</param>
    /// <returns>A disposable that cancels the scheduled work if disposed before it runs.</returns>
    IDisposable Schedule(Action action, TimeSpan dueTime);
}
