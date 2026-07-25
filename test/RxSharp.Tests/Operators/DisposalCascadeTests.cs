using RxSharp.Operators;

namespace RxSharp.Tests.Operators;

// Regression tests for the SubscribeChild fix (see CLAUDE.md Learnings): an operator with a single, stable
// inner subscription must register that inner subscriber as a child of its own downstream subscriber, so a
// disposal further down the chain (here, Take completing early) cascades all the way back to a fully
// synchronous, self-checking source mid-loop, instead of only after the whole call stack unwinds.
[TestFixture]
public class DisposalCascadeTests
{
    private static Observable<int> SynchronousObservable(List<int> sideEffects)
        => new(subscriber =>
        {
            for (var i = 0; !subscriber.IsDisposed && i < 10; i++)
            {
                sideEffects.Add(i);
                subscriber.OnNext(i);
            }
        });

    [Test]
    public void ShouldCascadeDisposalThroughMap()
    {
        var sideEffects = new List<int>();
        SynchronousObservable(sideEffects).Map(x => x * 10).Take(3).Subscribe(_ => { });

        Assert.That(sideEffects, Is.EqualTo(new[] { 0, 1, 2 }));
    }

    [Test]
    public void ShouldCascadeDisposalThroughFilter()
    {
        var sideEffects = new List<int>();
        SynchronousObservable(sideEffects).Filter(x => true).Take(3).Subscribe(_ => { });

        Assert.That(sideEffects, Is.EqualTo(new[] { 0, 1, 2 }));
    }

    [Test]
    public void ShouldCascadeDisposalThroughTap()
    {
        var sideEffects = new List<int>();
        SynchronousObservable(sideEffects).Tap(_ => { }).Take(3).Subscribe(_ => { });

        Assert.That(sideEffects, Is.EqualTo(new[] { 0, 1, 2 }));
    }

    [Test]
    public void ShouldCascadeDisposalThroughScan()
    {
        var sideEffects = new List<int>();
        SynchronousObservable(sideEffects).Scan((acc, x) => acc + x, 0).Take(3).Subscribe(_ => { });

        Assert.That(sideEffects, Is.EqualTo(new[] { 0, 1, 2 }));
    }

    [Test]
    public void ShouldCascadeDisposalThroughDistinctUntilChanged()
    {
        var sideEffects = new List<int>();
        SynchronousObservable(sideEffects).DistinctUntilChanged().Take(3).Subscribe(_ => { });

        Assert.That(sideEffects, Is.EqualTo(new[] { 0, 1, 2 }));
    }

    [Test]
    public void ShouldCascadeDisposalThroughMultipleChainedOperators()
    {
        var sideEffects = new List<int>();
        SynchronousObservable(sideEffects).Map(x => x).Filter(x => true).Tap(_ => { }).Take(3).Subscribe(_ => { });

        Assert.That(sideEffects, Is.EqualTo(new[] { 0, 1, 2 }));
    }
}
