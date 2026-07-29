using ReactiveExtensionsSharp.Operators;
using ReactiveExtensionsSharp.Subjects;

namespace ReactiveExtensionsSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/audit-spec.ts.
[TestFixture]
public class AuditTests
{
    [Test]
    public void ShouldEmitTheMostRecentValueOnceTheDurationWindowElapses()
    {
        var source = new Subject<int>();
        var results = new List<int>();
        using var signal = new ManualResetEventSlim();

        source.AsObservable().Audit(_ => Observable.Timer(TimeSpan.FromMilliseconds(30))).Subscribe(value =>
        {
            results.Add(value);
            signal.Set();
        });

        source.OnNext(1);
        source.OnNext(2);
        source.OnNext(3);

        Assert.That(signal.Wait(TimeSpan.FromSeconds(2)), Is.True);
        Assert.That(results, Is.EqualTo(new[] { 3 }), "audit ignores values while a window is open, unlike debounce it does not reset the window per value");
    }

    [Test]
    public void ShouldIgnoreValuesThatArriveWhileAWindowIsAlreadyOpenButStillOpenANewWindowAfterwards()
    {
        var source = new Subject<int>();
        var results = new List<int>();
        using var firstSignal = new ManualResetEventSlim();
        using var secondSignal = new ManualResetEventSlim();

        source.AsObservable().Audit(_ => Observable.Timer(TimeSpan.FromMilliseconds(30))).Subscribe(value =>
        {
            results.Add(value);
            if (results.Count == 1)
            {
                firstSignal.Set();
            }
            else
            {
                secondSignal.Set();
            }
        });

        source.OnNext(1);
        Assert.That(firstSignal.Wait(TimeSpan.FromSeconds(2)), Is.True);

        source.OnNext(2);
        Assert.That(secondSignal.Wait(TimeSpan.FromSeconds(2)), Is.True);

        Assert.That(results, Is.EqualTo(new[] { 1, 2 }));
    }

    [Test]
    public void ShouldEmitLastValueAfterDurationCompletesIfSourceCompletesFirst()
    {
        var source = new Subject<int>();
        var results = new List<int>();
        var completed = false;

        source.AsObservable().Audit(_ => Observable.Timer(TimeSpan.FromSeconds(5))).Subscribe(results.Add, onComplete: () => completed = true);

        source.OnNext(1);
        source.OnCompleted();

        Assert.That(results, Is.EqualTo(new[] { 1 }));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldNotEmitAnythingIfSourceCompletesWithoutHavingEmitted()
    {
        var source = new Subject<int>();
        var results = new List<int>();
        var completed = false;

        source.AsObservable().Audit(_ => Observable.Timer(TimeSpan.FromMilliseconds(30))).Subscribe(results.Add, onComplete: () => completed = true);

        source.OnCompleted();

        Assert.That(results, Is.Empty);
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldPropagateErrorsFromTheSourceImmediately()
    {
        var source = new Subject<int>();
        var error = new InvalidOperationException("boom");
        var results = new List<int>();
        Exception? received = null;

        source.AsObservable().Audit(_ => Observable.Timer(TimeSpan.FromSeconds(5))).Subscribe(results.Add, err => received = err);

        source.OnNext(1);
        source.OnError(error);

        Assert.That(results, Is.Empty);
        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldPropagateErrorsFromTheDurationObservable()
    {
        var source = new Subject<int>();
        var duration = new Subject<Unit>();
        var error = new InvalidOperationException("boom");
        Exception? received = null;

        source.AsObservable().Audit(_ => duration.AsObservable()).Subscribe(onError: err => received = err);

        source.OnNext(1);
        duration.OnError(error);

        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldPropagateErrorsThrownFromTheDurationSelector()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;

        Observable.Of(1).Audit<int, Unit>(_ => throw error).Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldMirrorSourceIfDurationsAreSynchronous()
    {
        var results = new List<int>();
        var completed = false;

        Observable.Of(1, 2, 3).Audit(_ => Observable.Of(0)).Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.EqualTo(new[] { 1, 2, 3 }));
        Assert.That(completed, Is.True);
    }

    // rxjs's "should stop listening to a synchronous observable when unsubscribed" test is not ported here:
    // it relies on a `Take`-driven synchronous unsubscribe interrupting a still-running upstream `Subscribe`
    // call, which this port's `SingleAssignmentDisposable`-based teardown does not support yet (the disposal
    // only takes effect once the nested synchronous `Subscribe` call unwinds, i.e. after it already ran to
    // completion) — a pre-existing gap in `Take` itself, reproducible with `Take` alone, not specific to `Audit`.

    // Regression test for the disposal-cascade fix (see CLAUDE.md Learnings): Audit's outer source subscription
    // must register its inner subscriber as a child of its own downstream subscriber, so a disposal further down
    // the chain (here, Take completing early) cascades all the way back to a fully synchronous, self-checking
    // source mid-loop. A synchronous duration selector (Observable.Of) opens and closes each window immediately,
    // so the whole chain is synchronous end-to-end.
    [Test]
    public void ShouldCascadeDisposalThroughAuditToASynchronousSource()
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

        source.Audit(_ => Observable.Of(0)).Take(3).Subscribe(_ => { });

        Assert.That(sideEffects, Is.EqualTo(new[] { 0, 1, 2 }));
    }
}
