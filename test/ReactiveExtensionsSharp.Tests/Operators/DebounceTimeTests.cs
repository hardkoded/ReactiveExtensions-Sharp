using ReactiveExtensionsSharp.Operators;
using ReactiveExtensionsSharp.Subjects;
using ReactiveExtensionsSharp.Testing;

namespace ReactiveExtensionsSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/debounceTime-spec.ts.
[TestFixture]
public class DebounceTimeTests
{
    [Test]
    public void ShouldEmitOnlyTheMostRecentValueAfterABurstGoesQuiet()
    {
        var source = new Subject<int>();
        var results = new List<int>();
        using var signal = new ManualResetEventSlim();

        source.AsObservable().DebounceTime(TimeSpan.FromMilliseconds(30)).Subscribe(value =>
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

    // Same scenario as above, but via TestScheduler: proves each burst value cancels/restarts the pending timer
    // (not just "eventually only 3 comes out", but "exactly one value, at exactly frame 30 — the cancelled
    // timers for 1 and 2 never fire").
    [Test]
    public void ShouldEmitOnlyTheMostRecentValueAfterABurstGoesQuiet_UsingVirtualTime()
    {
        var scheduler = new TestScheduler();
        var source = new Subject<int>();
        var due = TimeSpan.FromTicks(30);

        var results = scheduler.Record(source.AsObservable().DebounceTime(due, scheduler));

        source.OnNext(1);
        source.OnNext(2);
        source.OnNext(3);

        Assert.That(results, Is.Empty, "should not emit synchronously, even mid-burst");
        scheduler.Start();

        Assert.That(results, Is.EqualTo(new[] { Recorded.OnNext(due, 3) }));
    }

    [Test]
    public void ShouldEmitAnyPendingValueBeforeCompleting()
    {
        var source = new Subject<int>();
        var results = new List<int>();
        var completed = false;

        source.AsObservable().DebounceTime(TimeSpan.FromSeconds(5)).Subscribe(results.Add, onComplete: () => completed = true);

        source.OnNext(1);
        source.OnCompleted();

        Assert.That(results, Is.EqualTo(new[] { 1 }));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldPropagateErrorsImmediatelyWithoutEmittingThePendingValue()
    {
        var source = new Subject<int>();
        var error = new InvalidOperationException("boom");
        var results = new List<int>();
        Exception? received = null;

        source.AsObservable().DebounceTime(TimeSpan.FromSeconds(5)).Subscribe(results.Add, err => received = err);

        source.OnNext(1);
        source.OnError(error);

        Assert.That(results, Is.Empty);
        Assert.That(received, Is.SameAs(error));
    }

    // Regression test for the disposal-cascade fix (see CLAUDE.md Learnings): DebounceTime's outer source
    // subscription must register its inner subscriber as a child of its own downstream subscriber, so a
    // disposal further down the chain (here, Take completing early) cascades all the way back and tears down
    // the source subscription. Unlike Debounce/Audit/Throttle (which can be driven fully synchronously via
    // Observable.Of), DebounceTime always defers through a real scheduler, so this observes the cascade
    // indirectly: a tracking wrapper around the source registers a finalizer as a child of the subscriber
    // DebounceTime subscribes with, and asserts that finalizer runs once Take(1) completes the chain.
    [Test]
    public void ShouldCascadeDisposalThroughDebounceTimeToTheSourceSubscription()
    {
        var source = new Subject<int>();
        using var disposalSignal = new ManualResetEventSlim();
        var sourceUnsubscribed = false;

        // The signal is set from *inside* the finalizer itself (not from the value/onNext callback), so waiting
        // on it can't race with the cascade — unlike waiting on a signal set from the emitted value, which races
        // with Take's own subsequent Dispose() call happening on the same background thread just afterwards.
        Observable<int> trackedSource = new(subscriber =>
        {
            var innerSubscription = source.AsObservable().Subscribe(subscriber);
            subscriber.Add(new Subscription(() =>
            {
                sourceUnsubscribed = true;
                disposalSignal.Set();
            }));
            return innerSubscription;
        });

        trackedSource.DebounceTime(TimeSpan.FromMilliseconds(5)).Take(1).Subscribe(_ => { });

        source.OnNext(1);

        Assert.That(disposalSignal.Wait(TimeSpan.FromSeconds(2)), Is.True, "Take completing should cascade back and unsubscribe from the source");
        Assert.That(sourceUnsubscribed, Is.True);
    }
}
