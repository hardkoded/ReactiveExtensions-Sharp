using RxSharp.Operators;
using RxSharp.Subjects;

namespace RxSharp.Tests.Operators;

// Ported from rxjs 7.8.2 spec/operators/takeUntil-spec.ts (non-marble cases).
[TestFixture]
public class TakeUntilTests
{
    [Test]
    public void ShouldTakeValuesUntilNotifierEmits()
    {
        var source = new Subject<int>();
        var notifier = new Subject<int>();
        var results = new List<int>();
        var completed = false;

        source.AsObservable().TakeUntil(notifier.AsObservable()).Subscribe(results.Add, onComplete: () => completed = true);

        source.OnNext(1);
        source.OnNext(2);
        notifier.OnNext(0);
        source.OnNext(3);

        Assert.That(results, Is.EqualTo(new[] { 1, 2 }));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldPassAllValuesIfNotifierNeverEmits()
    {
        var results = new List<int>();
        Observable.Of(1, 2, 3).TakeUntil(Observable.Never<int>()).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 1, 2, 3 }));
    }

    [Test]
    public void ShouldCompleteImmediatelyIfNotifierEmitsSynchronouslyBeforeSourceSubscription()
    {
        var results = new List<int>();
        var completed = false;
        Observable.Of(1, 2, 3).TakeUntil(Observable.Of(0)).Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.Empty);
        Assert.That(completed, Is.True);
    }
}
