namespace RxSharp;

/// <summary>Schedules work after a delay. Mirrors rxjs's <c>SchedulerLike</c>. A minimal seam for now — see CLAUDE.md for the plan to grow this into a full scheduler hierarchy with a virtual-time implementation.</summary>
public interface IScheduler
{
    DateTimeOffset Now { get; }

    IDisposable Schedule(Action action, TimeSpan dueTime);
}
