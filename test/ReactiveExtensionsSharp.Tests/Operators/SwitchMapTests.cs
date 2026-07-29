using ReactiveExtensionsSharp.Operators;
using ReactiveExtensionsSharp.Subjects;

namespace ReactiveExtensionsSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/switchMap-spec.ts.
[TestFixture]
public class SwitchMapTests
{
    [Test]
    public void ShouldMapAndFlattenEachValue()
    {
        var results = new List<int>();
        Observable.Of(1, 2, 3).SwitchMap(x => Observable.Of(x, x * x, x * x * x)).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 1, 1, 1, 2, 4, 8, 3, 9, 27 }));
    }

    [Test]
    public void ShouldUnsubscribeFromThePreviousInnerWhenANewOneArrives()
    {
        var outer = new Subject<int>();
        var innerA = new Subject<string>();
        var innerB = new Subject<string>();
        var results = new List<string>();

        outer.AsObservable().SwitchMap(x => x == 1 ? innerA.AsObservable() : innerB.AsObservable()).Subscribe(results.Add);

        outer.OnNext(1);
        innerA.OnNext("a1");

        outer.OnNext(2);
        innerA.OnNext("a-should-be-ignored");
        innerB.OnNext("b1");

        Assert.That(results, Is.EqualTo(new[] { "a1", "b1" }));
    }

    [Test]
    public void ShouldCompleteWhenSourceAndCurrentInnerHaveBothCompleted()
    {
        var outer = new Subject<int>();
        var inner = new Subject<int>();
        var completed = false;

        outer.AsObservable().SwitchMap(_ => inner.AsObservable()).Subscribe(onComplete: () => completed = true);

        outer.OnNext(1);
        outer.OnCompleted();
        Assert.That(completed, Is.False);

        inner.OnCompleted();
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldPropagateErrorsFromTheProjection()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;
        Observable.Of(1).SwitchMap<int, int>(_ => throw error).Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }

    // Regression test for the disposal-cascade fix (see CLAUDE.md Learnings): a fully-synchronous, self-checking
    // source composed with SwitchMap through an early-completing downstream Take must stop mid-loop, not just
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

        source.SwitchMap(x => Observable.Of(x)).Take(3).Subscribe(_ => { });

        Assert.That(sideEffects, Is.EqualTo(new[] { 0, 1, 2 }));
    }
}
