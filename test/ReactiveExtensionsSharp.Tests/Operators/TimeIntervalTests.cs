using ReactiveExtensionsSharp.Operators;
using ReactiveExtensionsSharp.Subjects;
using ReactiveExtensionsSharp.Testing;

namespace ReactiveExtensionsSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/timeInterval-spec.ts, driven manually via a
// TestScheduler's virtual clock (advanced between pushes into a Subject) rather than marble syntax, for
// deterministic timing assertions without real wall-clock delays.
[TestFixture]
public class TimeIntervalTests
{
    [Test]
    public void ShouldTagEachValueWithTheElapsedTimeSincePreviousEmission()
    {
        var scheduler = new TestScheduler();
        var source = new Subject<int>();
        var results = new List<TimeInterval<int>>();

        source.AsObservable().TimeInterval(scheduler).Subscribe(results.Add);

        scheduler.AdvanceBy(TimeSpan.FromTicks(10));
        source.OnNext(1);
        scheduler.AdvanceBy(TimeSpan.FromTicks(20));
        source.OnNext(2);
        scheduler.AdvanceBy(TimeSpan.FromTicks(40));
        source.OnNext(3);

        Assert.That(results.Select(r => r.Value), Is.EqualTo(new[] { 1, 2, 3 }));
        Assert.That(
            results.Select(r => r.Interval),
            Is.EqualTo(new[] { TimeSpan.FromTicks(10), TimeSpan.FromTicks(20), TimeSpan.FromTicks(40) }));
    }

    [Test]
    public void ShouldForwardErrorsUnaffected()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;

        Observable.ThrowError<int>(() => error).TimeInterval().Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldCompleteWithoutEmittingOnAnEmptySource()
    {
        var completed = false;
        Observable.Empty<int>().TimeInterval().Subscribe(onComplete: () => completed = true);

        Assert.That(completed, Is.True);
    }
}
