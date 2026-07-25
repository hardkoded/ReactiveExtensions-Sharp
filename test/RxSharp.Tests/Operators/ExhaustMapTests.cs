using RxSharp.Operators;
using RxSharp.Subjects;

namespace RxSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/exhaustMap-spec.ts. The deprecated resultSelector
// overload is not ported (not part of this port's surface).
[TestFixture]
public class ExhaustMapTests
{
    [Test]
    public void ShouldMapAndFlattenEachItemToAnObservable()
    {
        var results = new List<int>();
        Observable.Of(1, 2, 3).ExhaustMap(x => Observable.Of(x, x * 10)).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 1, 10, 2, 20, 3, 30 }));
    }

    [Test]
    public void ShouldIgnoreSourceValuesWhileAnInnerObservableIsStillActive()
    {
        var source = new Subject<int>();
        var innerA = new Subject<string>();
        var innerB = new Subject<string>();
        var results = new List<string>();

        source.AsObservable().ExhaustMap(x => x == 1 ? innerA.AsObservable() : innerB.AsObservable()).Subscribe(results.Add);

        source.OnNext(1);
        innerA.OnNext("a1");

        // While innerA is still active, this second source value must be ignored entirely -- innerB must never
        // even be subscribed to.
        source.OnNext(2);
        innerB.OnNext("b-should-be-ignored");
        innerA.OnNext("a2");

        innerA.OnCompleted();

        // Now that innerA has completed, a new source value is free to start a new inner.
        source.OnNext(3);
        innerB.OnNext("b1");

        Assert.That(results, Is.EqualTo(new[] { "a1", "a2", "b1" }));
    }

    [Test]
    public void ShouldCompleteWhenSourceAndTheActiveInnerHaveBothCompleted()
    {
        var source = new Subject<int>();
        var inner = new Subject<int>();
        var completed = false;

        source.AsObservable().ExhaustMap(_ => inner.AsObservable()).Subscribe(onComplete: () => completed = true);

        source.OnNext(1);
        source.OnCompleted();
        Assert.That(completed, Is.False);

        inner.OnCompleted();
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldCompleteImmediatelyIfSourceCompletesWithNoActiveInner()
    {
        var completed = false;
        Observable.Empty<int>().ExhaustMap(_ => Observable.Of(1)).Subscribe(onComplete: () => completed = true);

        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldForwardAnErrorFromTheSource()
    {
        var error = new InvalidOperationException("boom");
        var innerSubscribed = false;
        Exception? received = null;

        Observable.ThrowError<int>(() => error).ExhaustMap(_ =>
        {
            innerSubscribed = true;
            return Observable.Of(1);
        }).Subscribe(onError: err => received = err);

        Assert.That(innerSubscribed, Is.False);
        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldForwardAnErrorFromTheActiveInner()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;

        Observable.Of(1).ExhaustMap(_ => Observable.ThrowError<int>(() => error)).Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldForwardAnErrorThrownByTheProjection()
    {
        var results = new List<int>();
        Exception? received = null;

        Observable.Of(1, 2).ExhaustMap<int, int>(_ => throw new InvalidOperationException("bad"))
            .Subscribe(results.Add, onError: err => received = err);

        Assert.That(results, Is.Empty);
        Assert.That(received, Is.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void ShouldOnlyAdvanceTheProjectionIndexForAcceptedValues()
    {
        var seenIndices = new List<int>();
        var source = new Subject<int>();
        var inner = new Subject<int>();

        source.AsObservable().ExhaustMap((value, index) =>
        {
            seenIndices.Add(index);
            return inner.AsObservable();
        }).Subscribe();

        source.OnNext(1); // accepted, index 0
        source.OnNext(2); // ignored, inner still active
        inner.OnCompleted();
        source.OnNext(3); // accepted, index 1

        Assert.That(seenIndices, Is.EqualTo(new[] { 0, 1 }));
    }

    [Test]
    public void ShouldIgnoreSubsequentSynchronousReentrancesWhileSubscribingTheInner()
    {
        var source = new BehaviorSubject<int>(1);
        var results = new List<int>();

        source.AsObservable().Take(3).ExhaustMap(value => new Observable<int>(subscriber =>
        {
            // Reentrant: this fires before the inner subscription below has even finished being set up, so it
            // must be ignored (an active inner -- this very one -- is already in flight).
            source.OnNext(value + 1);
            subscriber.OnNext(value);
        })).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 1 }));
    }

    // rxjs's two "should stop listening to a synchronous observable when unsubscribed" tests are represented
    // here by the Take(3) variant below, matching the idiom used across this port's own disposal-cascade
    // regression tests (see CLAUDE.md Learnings and DisposalCascadeTests.cs). This now passes: ExhaustMap's
    // outer source subscription is registered as a child of its downstream subscriber (via SubscribeChild)
    // before the source's own Subscribe call runs, so a downstream Take completing cascades back and stops the
    // loop mid-iteration instead of only after the whole synchronous call stack unwinds.
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

        source.ExhaustMap(x => Observable.Of(x)).Take(3).Subscribe(_ => { });

        Assert.That(sideEffects, Is.EqualTo(new[] { 0, 1, 2 }));
    }
}
