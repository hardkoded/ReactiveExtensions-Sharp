using RxSharp.Operators;
using RxSharp.Subjects;
using RxSharp.Testing;

namespace RxSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/timestamp-spec.ts, driven manually via a
// TestScheduler's virtual clock rather than marble syntax.
[TestFixture]
public class TimestampTests
{
    [Test]
    public void ShouldTagEachValueWithTheSchedulersCurrentTime()
    {
        var scheduler = new TestScheduler();
        var source = new Subject<int>();
        var results = new List<Timestamp<int>>();

        source.AsObservable().Timestamp(scheduler).Subscribe(results.Add);

        scheduler.AdvanceBy(TimeSpan.FromTicks(10));
        source.OnNext(1);
        scheduler.AdvanceBy(TimeSpan.FromTicks(20));
        source.OnNext(2);

        Assert.That(results.Select(r => r.Value), Is.EqualTo(new[] { 1, 2 }));
        Assert.That(
            results.Select(r => r.TimestampValue),
            Is.EqualTo(new[] { new DateTimeOffset(10, TimeSpan.Zero), new DateTimeOffset(30, TimeSpan.Zero) }));
    }

    [Test]
    public void ShouldForwardErrorsUnaffected()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;

        Observable.ThrowError<int>(() => error).Timestamp().Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldCompleteWithoutEmittingOnAnEmptySource()
    {
        var completed = false;
        Observable.Empty<int>().Timestamp().Subscribe(onComplete: () => completed = true);

        Assert.That(completed, Is.True);
    }
}
