using RxSharp.Operators;
using RxSharp.Subjects;

namespace RxSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/auditTime-spec.ts.
[TestFixture]
public class AuditTimeTests
{
    [Test]
    public void ShouldEmitTheLastValueInEachTimeWindow()
    {
        var source = new Subject<int>();
        var results = new List<int>();
        using var signal = new ManualResetEventSlim();

        source.AsObservable().AuditTime(TimeSpan.FromMilliseconds(30)).Subscribe(value =>
        {
            results.Add(value);
            signal.Set();
        });

        source.OnNext(1);
        source.OnNext(2);
        source.OnNext(3);

        Assert.That(signal.Wait(TimeSpan.FromSeconds(2)), Is.True);
        Assert.That(results, Is.EqualTo(new[] { 3 }));
    }

    [Test]
    public void ShouldAuditTimeEventsAndOnlyEmitTheLastOne()
    {
        var results = new List<int>();
        using var signal = new ManualResetEventSlim();

        Observable.Of(1, 2, 3).AuditTime(TimeSpan.FromMilliseconds(5)).Subscribe(value =>
        {
            results.Add(value);
            signal.Set();
        });

        Assert.That(signal.Wait(TimeSpan.FromSeconds(2)), Is.True);
        Assert.That(results, Is.EqualTo(new[] { 3 }));
    }

    [Test]
    public void ShouldDelayTheSourceIfValuesAreNotEmittedOftenEnough()
    {
        var source = new Subject<int>();
        var results = new List<int>();
        using var signal = new ManualResetEventSlim();

        source.AsObservable().AuditTime(TimeSpan.FromMilliseconds(30)).Subscribe(value =>
        {
            results.Add(value);
            signal.Set();
        });

        source.OnNext(1);

        Assert.That(signal.Wait(TimeSpan.FromSeconds(2)), Is.True);
        Assert.That(results, Is.EqualTo(new[] { 1 }));
    }

    [Test]
    public void ShouldCompleteWhenSourceDoesNotEmit()
    {
        var source = new Subject<int>();
        var results = new List<int>();
        var completed = false;

        source.AsObservable().AuditTime(TimeSpan.FromMilliseconds(20)).Subscribe(results.Add, onComplete: () => completed = true);

        source.OnCompleted();

        Assert.That(results, Is.Empty);
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldRaiseErrorWhenSourceRaisesError()
    {
        var source = new Subject<int>();
        var error = new InvalidOperationException("boom");
        Exception? received = null;

        source.AsObservable().AuditTime(TimeSpan.FromMilliseconds(20)).Subscribe(onError: err => received = err);

        source.OnError(error);

        Assert.That(received, Is.SameAs(error));
    }
}
