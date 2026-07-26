using RxSharp.Operators;
using RxSharp.Subjects;

namespace RxSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/switchScan-spec.ts.
[TestFixture]
public class SwitchScanTests
{
    [Test]
    public void ShouldAccumulateAcrossSwitchedInnerObservables()
    {
        var results = new List<int>();
        var completed = false;

        Observable.Of(1, 2, 3)
            .SwitchScan((acc, value) => Observable.Of(acc + value), 0)
            .Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.EqualTo(new[] { 1, 3, 6 }));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldUnsubscribeFromThePreviousInnerWhenANewOneArrives()
    {
        var source = new Subject<int>();
        var innerA = new Subject<int>();
        var innerB = new Subject<int>();
        var results = new List<int>();

        source.AsObservable().SwitchScan((_, value) => value == 1 ? innerA.AsObservable() : innerB.AsObservable(), 0).Subscribe(results.Add);

        source.OnNext(1);
        innerA.OnNext(10);
        source.OnNext(2);
        innerA.OnNext(99);
        innerB.OnNext(20);

        Assert.That(results, Is.EqualTo(new[] { 10, 20 }));
    }

    [Test]
    public void ShouldForwardErrorThrownFromTheAccumulator()
    {
        var thrown = new InvalidOperationException("boom");
        Exception? received = null;

        Observable.Of(1).SwitchScan<int, int>((_, _) => throw thrown, 0).Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(thrown));
    }

    [Test]
    public void ShouldForwardAnErrorFromTheActiveInnerObservable()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;

        Observable.Of(1).SwitchScan((_, _) => Observable.ThrowError<int>(() => error), 0).Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }
}
