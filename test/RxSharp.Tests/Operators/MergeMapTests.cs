using RxSharp.Operators;

namespace RxSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/mergeMap-spec.ts.
[TestFixture]
public class MergeMapTests
{
    [Test]
    public void ShouldMapAndFlattenEachValue()
    {
        var results = new List<int>();
        var completed = false;
        Observable.Of(1, 2, 3).MergeMap(x => Observable.Of(x, x * 10)).Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.EquivalentTo(new[] { 1, 10, 2, 20, 3, 30 }));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldPropagateErrorsFromTheProjection()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;
        Observable.Of(1).MergeMap<int, int>(_ => throw error).Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldPropagateErrorsFromAnInnerObservable()
    {
        var error = new InvalidOperationException("inner boom");
        Exception? received = null;
        Observable.Of(1, 2).MergeMap(_ => Observable.ThrowError<int>(() => error)).Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldOnlyCompleteWhenTheSourceAndAllInnersHaveCompleted()
    {
        var subjectA = new RxSharp.Subjects.Subject<int>();
        var subjectB = new RxSharp.Subjects.Subject<int>();
        var completed = false;

        Observable.Of(subjectA.AsObservable(), subjectB.AsObservable())
            .MergeMap(inner => inner)
            .Subscribe(onComplete: () => completed = true);

        subjectA.OnCompleted();
        Assert.That(completed, Is.False);

        subjectB.OnCompleted();
        Assert.That(completed, Is.True);
    }

    // Regression test for the disposal-cascade fix (see CLAUDE.md Learnings): a fully-synchronous, self-checking
    // source composed with MergeMap through an early-completing downstream Take must stop mid-loop, not just
    // after the whole synchronous call stack unwinds. Mirrors DisposalCascadeTests.cs's pattern.
    [Test]
    public void ShouldCascadeDisposalToASynchronousSourceThroughTake()
    {
        var sideEffects = new List<int>();
        Observable<int> source = new(subscriber =>
        {
            for (var i = 0; !subscriber.IsDisposed && i < 10; i++)
            {
                sideEffects.Add(i);
                subscriber.OnNext(i);
            }
        });

        source.MergeMap(x => Observable.Of(x)).Take(3).Subscribe(_ => { });

        Assert.That(sideEffects, Is.EqualTo(new[] { 0, 1, 2 }));
    }
}
