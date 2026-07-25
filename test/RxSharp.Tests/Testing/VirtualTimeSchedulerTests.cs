using RxSharp.Testing;

namespace RxSharp.Tests.Testing;

// No upstream rxjs spec equivalent — marble-testing-the-marble-tester isn't a thing rxjs itself ports from.
// Written from scratch to pin down the virtual-time queue's correctness guarantees: ordering, no missed/duplicate
// executions, and disposal/cancellation of scheduled actions.
[TestFixture]
public class VirtualTimeSchedulerTests
{
    [Test]
    public void ClockStartsAtZero()
    {
        var scheduler = new VirtualTimeScheduler();
        Assert.That(scheduler.Clock, Is.EqualTo(TimeSpan.Zero));
    }

    [Test]
    public void ScheduleDoesNotRunTheActionSynchronously()
    {
        var scheduler = new VirtualTimeScheduler();
        var ran = false;

        scheduler.Schedule(() => ran = true, TimeSpan.FromTicks(5));

        Assert.That(ran, Is.False);
        Assert.That(scheduler.Clock, Is.EqualTo(TimeSpan.Zero));
    }

    [Test]
    public void ActionsScheduledForTheSameVirtualTimeExecuteInInsertionOrder()
    {
        var scheduler = new VirtualTimeScheduler();
        var order = new List<int>();

        scheduler.Schedule(() => order.Add(1), TimeSpan.FromTicks(10));
        scheduler.Schedule(() => order.Add(2), TimeSpan.FromTicks(10));
        scheduler.Schedule(() => order.Add(3), TimeSpan.FromTicks(10));

        scheduler.AdvanceTo(TimeSpan.FromTicks(10));

        Assert.That(order, Is.EqualTo(new[] { 1, 2, 3 }));
    }

    [Test]
    public void ActionsScheduledOutOfOrderExecuteInDueTimeOrder()
    {
        var scheduler = new VirtualTimeScheduler();
        var order = new List<string>();

        scheduler.Schedule(() => order.Add("third"), TimeSpan.FromTicks(30));
        scheduler.Schedule(() => order.Add("first"), TimeSpan.FromTicks(10));
        scheduler.Schedule(() => order.Add("second"), TimeSpan.FromTicks(20));

        scheduler.AdvanceTo(TimeSpan.FromTicks(30));

        Assert.That(order, Is.EqualTo(new[] { "first", "second", "third" }));
    }

    [Test]
    public void AdvanceToOnlyExecutesActionsUpToAndIncludingTheTargetTime()
    {
        var scheduler = new VirtualTimeScheduler();
        var order = new List<int>();

        scheduler.Schedule(() => order.Add(1), TimeSpan.FromTicks(5));
        scheduler.Schedule(() => order.Add(2), TimeSpan.FromTicks(10));
        scheduler.Schedule(() => order.Add(3), TimeSpan.FromTicks(15));

        scheduler.AdvanceTo(TimeSpan.FromTicks(10));

        Assert.That(order, Is.EqualTo(new[] { 1, 2 }), "should run everything due at or before frame 10");
        Assert.That(scheduler.Clock, Is.EqualTo(TimeSpan.FromTicks(10)));

        scheduler.AdvanceTo(TimeSpan.FromTicks(15));

        Assert.That(order, Is.EqualTo(new[] { 1, 2, 3 }), "should run the remainder once advanced past it");
    }

    [Test]
    public void AdvanceToNeverRunsAnActionPastTheTargetTimeEvenIfNothingIsDue()
    {
        var scheduler = new VirtualTimeScheduler();
        var ran = false;

        scheduler.Schedule(() => ran = true, TimeSpan.FromTicks(100));
        scheduler.AdvanceTo(TimeSpan.FromTicks(50));

        Assert.That(ran, Is.False);
        Assert.That(scheduler.Clock, Is.EqualTo(TimeSpan.FromTicks(50)), "the clock should still land exactly on the target");
    }

    [Test]
    public void AdvanceByMovesTheClockRelativeToItsCurrentValue()
    {
        var scheduler = new VirtualTimeScheduler();
        var order = new List<int>();

        scheduler.Schedule(() => order.Add(1), TimeSpan.FromTicks(10));
        scheduler.AdvanceBy(TimeSpan.FromTicks(5));

        Assert.That(order, Is.Empty);
        Assert.That(scheduler.Clock, Is.EqualTo(TimeSpan.FromTicks(5)));

        scheduler.AdvanceBy(TimeSpan.FromTicks(5));

        Assert.That(order, Is.EqualTo(new[] { 1 }));
        Assert.That(scheduler.Clock, Is.EqualTo(TimeSpan.FromTicks(10)));
    }

    [Test]
    public void AdvanceByRejectsNegativeAmounts()
        => Assert.Throws<ArgumentOutOfRangeException>(() => new VirtualTimeScheduler().AdvanceBy(TimeSpan.FromTicks(-1)));

    [Test]
    public void AdvanceToRejectsMovingTheClockBackwards()
    {
        var scheduler = new VirtualTimeScheduler();
        scheduler.AdvanceTo(TimeSpan.FromTicks(10));

        Assert.Throws<ArgumentOutOfRangeException>(() => scheduler.AdvanceTo(TimeSpan.FromTicks(5)));
    }

    [Test]
    public void DisposingTheReturnedHandleCancelsAnNotYetFiredAction()
    {
        var scheduler = new VirtualTimeScheduler();
        var ran = false;

        var handle = scheduler.Schedule(() => ran = true, TimeSpan.FromTicks(10));
        handle.Dispose();

        scheduler.AdvanceTo(TimeSpan.FromTicks(10));

        Assert.That(ran, Is.False);
    }

    [Test]
    public void CancellingOneActionDoesNotAffectOthersDueAtTheSameTime()
    {
        var scheduler = new VirtualTimeScheduler();
        var order = new List<int>();

        var handle = scheduler.Schedule(() => order.Add(1), TimeSpan.FromTicks(10));
        scheduler.Schedule(() => order.Add(2), TimeSpan.FromTicks(10));
        handle.Dispose();

        scheduler.AdvanceTo(TimeSpan.FromTicks(10));

        Assert.That(order, Is.EqualTo(new[] { 2 }));
    }

    [Test]
    public void EachActionRunsExactlyOnceAcrossMultipleAdvanceCalls()
    {
        var scheduler = new VirtualTimeScheduler();
        var runCounts = new int[3];

        scheduler.Schedule(() => runCounts[0]++, TimeSpan.FromTicks(5));
        scheduler.Schedule(() => runCounts[1]++, TimeSpan.FromTicks(10));
        scheduler.Schedule(() => runCounts[2]++, TimeSpan.FromTicks(10));

        scheduler.AdvanceTo(TimeSpan.FromTicks(5));
        scheduler.AdvanceTo(TimeSpan.FromTicks(10));
        scheduler.AdvanceTo(TimeSpan.FromTicks(20));

        Assert.That(runCounts, Is.EqualTo(new[] { 1, 1, 1 }), "no action should run twice or be skipped");
    }

    [Test]
    public void AnActionCanReScheduleMoreWorkAndItIsPickedUpWithinTheSameAdvanceCall()
    {
        var scheduler = new VirtualTimeScheduler();
        var order = new List<int>();

        void ScheduleChain(int remaining)
        {
            if (remaining == 0)
            {
                return;
            }

            order.Add(remaining);
            scheduler.Schedule(() => ScheduleChain(remaining - 1), TimeSpan.FromTicks(1));
        }

        scheduler.Schedule(() => ScheduleChain(3), TimeSpan.FromTicks(1));
        scheduler.AdvanceTo(TimeSpan.FromTicks(10));

        Assert.That(order, Is.EqualTo(new[] { 3, 2, 1 }));
    }

    [Test]
    public void StartDrainsEveryQueuedActionRegardlessOfHowFarOutItIsDue()
    {
        var scheduler = new VirtualTimeScheduler();
        var order = new List<int>();

        scheduler.Schedule(() => order.Add(1), TimeSpan.FromTicks(1_000_000));
        scheduler.Schedule(() => order.Add(2), TimeSpan.FromTicks(1));

        scheduler.Start();

        Assert.That(order, Is.EqualTo(new[] { 2, 1 }));
    }

    [Test]
    public void StartThrowsIfAnActionReschedulesItselfForever()
    {
        var scheduler = new VirtualTimeScheduler();

        void Loop() => scheduler.Schedule(Loop, TimeSpan.FromTicks(1));
        scheduler.Schedule(Loop, TimeSpan.FromTicks(1));

        Assert.Throws<InvalidOperationException>(() => scheduler.Start());
    }

    [Test]
    public void NowTracksTheVirtualClockNotRealTime()
    {
        var scheduler = new VirtualTimeScheduler();
        var before = scheduler.Now;

        scheduler.AdvanceTo(TimeSpan.FromTicks(1000));

        Assert.That(scheduler.Now, Is.GreaterThan(before));
        Assert.That(scheduler.Now.Ticks, Is.EqualTo(1000));
    }

    [Test]
    public void NegativeDueTimeIsClampedToNow()
    {
        var scheduler = new VirtualTimeScheduler();
        scheduler.AdvanceTo(TimeSpan.FromTicks(10));

        var ran = false;
        scheduler.Schedule(() => ran = true, TimeSpan.FromTicks(-5));

        scheduler.AdvanceBy(TimeSpan.Zero);

        Assert.That(ran, Is.True);
    }
}
