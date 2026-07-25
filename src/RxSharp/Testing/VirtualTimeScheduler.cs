namespace RxSharp.Testing;

/// <summary>
/// An <see cref="IScheduler"/> that tracks a virtual clock instead of real time. <see cref="Schedule"/> never
/// runs anything itself — it queues <c>(dueTime, action)</c> pairs. Nothing executes until <see cref="AdvanceTo"/>
/// or <see cref="AdvanceBy"/> moves the clock forward past their due time, at which point every action whose due
/// time has now been reached runs synchronously, in due-time order (ties broken by scheduling order). That
/// determinism is the entire point: it lets time-based operators (<c>Delay</c>, <c>DebounceTime</c>, <c>Timer</c>,
/// <c>Retry</c>'s backoff, ...) be tested without real <c>Thread.Sleep</c>/<c>ManualResetEventSlim</c> waits.
/// Mirrors rxjs's <c>VirtualTimeScheduler</c>.
/// </summary>
public class VirtualTimeScheduler : IScheduler
{
    private const int MaxDrainIterations = 100_000;

    private readonly List<ScheduledAction> _queue = new List<ScheduledAction>();
    private long _nextSequence;
    private TimeSpan _clock = TimeSpan.Zero;

    /// <summary>The current virtual time. Only ever moves forward, and only via <see cref="AdvanceTo"/>/<see cref="AdvanceBy"/>/<see cref="Start"/> — never on its own.</summary>
    public TimeSpan Clock => _clock;

    /// <summary>
    /// <see cref="IScheduler.Now"/>, expressed as the virtual clock offset from the Unix epoch, purely to satisfy
    /// the <see cref="IScheduler"/> contract (some future operator might read <c>Now</c> instead of taking a
    /// relative due time). Prefer reading <see cref="Clock"/> directly in tests — it is the same value without
    /// the epoch indirection.
    /// </summary>
    public DateTimeOffset Now => new DateTimeOffset(_clock.Ticks, TimeSpan.Zero);

    /// <summary>Queues <paramref name="action"/> to run once the clock reaches <see cref="Clock"/> + <paramref name="dueTime"/>. Returns a disposable that cancels the action if it hasn't run yet.</summary>
    public IDisposable Schedule(Action action, TimeSpan dueTime)
    {
        if (action is null)
        {
            throw new ArgumentNullException(nameof(action));
        }

        var delay = dueTime < TimeSpan.Zero ? TimeSpan.Zero : dueTime;
        var scheduled = new ScheduledAction(_clock + delay, _nextSequence++, action);
        _queue.Add(scheduled);
        return new Subscription(() => scheduled.IsCancelled = true);
    }

    /// <summary>Moves the clock forward by <paramref name="time"/> and runs every action due by the new clock value. Equivalent to <c>AdvanceTo(Clock + time)</c>.</summary>
    public void AdvanceBy(TimeSpan time)
    {
        if (time < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(time), time, "Cannot advance the virtual clock by a negative amount.");
        }

        AdvanceTo(_clock + time);
    }

    /// <summary>
    /// Moves the clock forward to <paramref name="time"/>, running every queued action whose due time is at or
    /// before <paramref name="time"/>, in due-time order (ties broken by scheduling order). Actions due strictly
    /// after <paramref name="time"/> are left queued untouched — advancing never runs anything past the target.
    /// The clock always ends exactly at <paramref name="time"/>, even if nothing was queued to run.
    /// </summary>
    public void AdvanceTo(TimeSpan time)
    {
        if (time < _clock)
        {
            throw new ArgumentOutOfRangeException(nameof(time), time, "Cannot move the virtual clock backwards.");
        }

        Drain(time);
        _clock = time;
    }

    /// <summary>
    /// Runs every currently- and subsequently-queued action to completion, however far into virtual time that
    /// takes (i.e. drains the queue, unlike <see cref="AdvanceTo"/> which stops at a fixed target). Guards
    /// against an action that reschedules itself forever with a hard iteration cap.
    /// </summary>
    public void Start() => Drain(TimeSpan.MaxValue);

    /// <summary>
    /// Executes queued actions due at or before <paramref name="limit"/>, in due-time/scheduling-order, until
    /// none remain (an action may itself schedule more work for the same drain pass — e.g. an operator's
    /// internal recursive timer chain — and that new work is picked up too, as long as it is still due within
    /// <paramref name="limit"/>). Does not itself move <see cref="_clock"/> past the last action it actually ran;
    /// callers that need the clock to land exactly on a target value (<see cref="AdvanceTo"/>) do that themselves.
    /// </summary>
    private void Drain(TimeSpan limit)
    {
        var iterations = 0;
        while (true)
        {
            var next = DequeueNextDue(limit);
            if (next is null)
            {
                return;
            }

            if (++iterations > MaxDrainIterations)
            {
                throw new InvalidOperationException(
                    $"VirtualTimeScheduler did not drain its queue after {MaxDrainIterations} iterations — an action is probably rescheduling itself forever.");
            }

            _clock = next.DueTime;
            if (!next.IsCancelled)
            {
                next.Action();
            }
        }
    }

    /// <summary>Finds, removes, and returns the earliest not-yet-cancelled action due at or before <paramref name="limit"/> (ties broken by scheduling order), or <c>null</c> if none qualifies.</summary>
    private ScheduledAction? DequeueNextDue(TimeSpan limit)
    {
        ScheduledAction? best = null;
        foreach (var candidate in _queue)
        {
            if (candidate.IsCancelled || candidate.DueTime > limit)
            {
                continue;
            }

            if (best is null || candidate.DueTime < best.DueTime || (candidate.DueTime == best.DueTime && candidate.Sequence < best.Sequence))
            {
                best = candidate;
            }
        }

        if (best is not null)
        {
            _queue.Remove(best);
        }

        return best;
    }

    private sealed class ScheduledAction
    {
        public ScheduledAction(TimeSpan dueTime, long sequence, Action action)
        {
            DueTime = dueTime;
            Sequence = sequence;
            Action = action;
        }

        public TimeSpan DueTime { get; }

        public long Sequence { get; }

        public Action Action { get; }

        public bool IsCancelled { get; set; }
    }
}
