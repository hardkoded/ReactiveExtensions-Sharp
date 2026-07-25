using RxSharp.Operators;
using RxSharp.Subjects;

namespace RxSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/throttle-spec.ts.
[TestFixture]
public class ThrottleTests
{
    [Test]
    public void ShouldImmediatelyEmitTheFirstValueInEachTimeWindowByDefault()
    {
        var source = new Subject<int>();
        var results = new List<int>();

        source.AsObservable().Throttle(_ => Observable.Timer(TimeSpan.FromMilliseconds(30))).Subscribe(results.Add);

        source.OnNext(1);
        source.OnNext(2);
        source.OnNext(3);

        Assert.That(results, Is.EqualTo(new[] { 1 }), "leading edge emits immediately; values during the window are dropped by default");
    }

    [Test]
    public void ShouldStartANewWindowAfterThePreviousOneClosesAndAllowANewLeadingValueThrough()
    {
        var source = new Subject<int>();
        var results = new List<int>();
        using var signal = new ManualResetEventSlim();

        source.AsObservable().Throttle(_ => Observable.Timer(TimeSpan.FromMilliseconds(30))).Subscribe(value =>
        {
            results.Add(value);
            signal.Set();
        });

        source.OnNext(1);
        Assert.That(results, Is.EqualTo(new[] { 1 }));

        source.OnNext(2);
        Assert.That(results, Is.EqualTo(new[] { 1 }), "still inside the first window, so this value is dropped");

        Thread.Sleep(TimeSpan.FromMilliseconds(60)); // let the window elapse fully

        signal.Reset();
        source.OnNext(3);
        Assert.That(signal.Wait(TimeSpan.FromSeconds(2)), Is.True);
        Assert.That(results, Is.EqualTo(new[] { 1, 3 }), "a new window opened, so the next value is emitted (leading edge) again");
    }

    [Test]
    public void WithTrailingEnabledShouldEmitTheMostRecentValueWhenTheWindowCloses()
    {
        var source = new Subject<int>();
        var results = new List<int>();
        using var signal = new ManualResetEventSlim();

        source.AsObservable().Throttle(_ => Observable.Timer(TimeSpan.FromMilliseconds(30)), leading: true, trailing: true).Subscribe(value =>
        {
            results.Add(value);
            if (results.Count == 2)
            {
                signal.Set();
            }
        });

        source.OnNext(1);
        source.OnNext(2);
        source.OnNext(3);

        Assert.That(signal.Wait(TimeSpan.FromSeconds(2)), Is.True);
        Assert.That(results, Is.EqualTo(new[] { 1, 3 }), "leading value immediately, then the most recent value once the window closes");
    }

    [Test]
    public void ShouldHandleSyncSourceWithSyncNotifierAndTrailingAppropriately()
    {
        var results = new List<object>();

        Observable.Of(1).Throttle(_ => Observable.Of(1), leading: false, trailing: true).Subscribe(
            value => results.Add(value),
            onComplete: () => results.Add("done"));

        Assert.That(results, Is.EqualTo(new object[] { 1, "done" }));
    }

    [Test]
    public void ShouldPropagateErrorsThrownFromTheDurationSelector()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;

        Observable.Of(1).Throttle<int, Unit>(_ => throw error).Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldPropagateErrorsFromTheDurationObservable()
    {
        var source = new Subject<int>();
        var duration = new Subject<Unit>();
        var error = new InvalidOperationException("boom");
        Exception? received = null;

        // Use a Subject-backed source (rather than `Of`) so the source does not complete synchronously right
        // after emitting — that would tear down the still-open throttle window (and its duration subscription)
        // before this test gets a chance to error the duration observable.
        source.AsObservable().Throttle(_ => duration.AsObservable()).Subscribe(onError: err => received = err);

        source.OnNext(1);
        duration.OnError(error);

        Assert.That(received, Is.SameAs(error));
    }

    // rxjs's "should stop listening to a synchronous observable when unsubscribed" test is not ported here:
    // it relies on a `Take`-driven synchronous unsubscribe interrupting a still-running upstream `Subscribe`
    // call, which this port's `SingleAssignmentDisposable`-based teardown does not support yet (the disposal
    // only takes effect once the nested synchronous `Subscribe` call unwinds, i.e. after it already ran to
    // completion) — a pre-existing gap in `Take` itself, reproducible with `Take` alone, not specific to `Throttle`.

    // Regression test for the disposal-cascade fix (see CLAUDE.md Learnings): Throttle's outer source
    // subscription must register its inner subscriber as a child of its own downstream subscriber, so a
    // disposal further down the chain (here, Take completing early) cascades all the way back to a fully
    // synchronous, self-checking source mid-loop. With the default leading:true/trailing:false config and a
    // synchronous duration selector (Observable.Of), each window opens and closes immediately, so every value
    // is forwarded and the whole chain is synchronous end-to-end.
    [Test]
    public void ShouldCascadeDisposalThroughThrottleToASynchronousSource()
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

        source.Throttle(_ => Observable.Of(0)).Take(3).Subscribe(_ => { });

        Assert.That(sideEffects, Is.EqualTo(new[] { 0, 1, 2 }));
    }
}
