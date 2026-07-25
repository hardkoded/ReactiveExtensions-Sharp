using RxSharp.Operators;
using RxSharp.Testing;

namespace RxSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/delay-spec.ts.
[TestFixture]
public class DelayTests
{
    [Test]
    public void ShouldDelayEachValueByTheGivenTimeSpan()
    {
        using var signal = new ManualResetEventSlim();
        var results = new List<int>();

        Observable.Of(1, 2, 3).Delay(TimeSpan.FromMilliseconds(20)).Subscribe(results.Add, onComplete: () => signal.Set());

        Assert.That(results, Is.Empty, "should not emit synchronously");
        Assert.That(signal.Wait(TimeSpan.FromSeconds(2)), Is.True);
        Assert.That(results, Is.EqualTo(new[] { 1, 2, 3 }));
    }

    // Same scenario as above, but via TestScheduler instead of real timers: deterministic, and it also pins down
    // the exact virtual timestamps (see Delay.cs's documented "latencies stack under a synchronous burst" behavior
    // from the CLAUDE.md Delay learning — item N of a synchronous burst fires at N x due, not at a uniform offset).
    [Test]
    public void ShouldDelayEachValueByTheGivenTimeSpan_UsingVirtualTime()
    {
        var scheduler = new TestScheduler();
        var due = TimeSpan.FromTicks(20);

        var results = scheduler.Record(Observable.Of(1, 2, 3).Delay(due, scheduler));

        Assert.That(results, Is.Empty, "should not emit synchronously");
        scheduler.Start();

        Assert.That(
            results,
            Is.EqualTo(new[]
            {
                Recorded.OnNext(TimeSpan.FromTicks(20), 1),
                Recorded.OnNext(TimeSpan.FromTicks(40), 2),
                Recorded.OnNext(TimeSpan.FromTicks(60), 3),
                Recorded.OnCompleted<int>(TimeSpan.FromTicks(60)),
            }));
    }

    [Test]
    public void ShouldPropagateErrorsFromTheSource()
    {
        using var signal = new ManualResetEventSlim();
        var error = new InvalidOperationException("boom");
        Exception? received = null;

        Observable.ThrowError<int>(() => error).Delay(TimeSpan.FromMilliseconds(10)).Subscribe(onError: err =>
        {
            received = err;
            signal.Set();
        });

        Assert.That(signal.Wait(TimeSpan.FromSeconds(2)), Is.True);
        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldPropagateErrorsFromTheSource_UsingVirtualTime()
    {
        var scheduler = new TestScheduler();
        var error = new InvalidOperationException("boom");

        // Delay.cs forwards source errors straight through (see its onError passthrough) rather than queuing
        // them like values — so unlike the OnNext values above, the error surfaces immediately, at frame 0,
        // not after the given due time. Virtual time makes this precise timing assertion possible; a real-timer
        // test could only ever confirm "eventually", not "exactly when".
        var results = scheduler.Record(Observable.ThrowError<int>(() => error).Delay(TimeSpan.FromTicks(10), scheduler));

        Assert.That(results, Is.EqualTo(new[] { Recorded.OnError<int>(TimeSpan.Zero, error) }), "errors are not delayed, only values are");
    }
}
