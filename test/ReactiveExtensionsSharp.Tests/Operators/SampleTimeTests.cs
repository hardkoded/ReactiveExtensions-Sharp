using ReactiveExtensionsSharp.Operators;
using ReactiveExtensionsSharp.Subjects;

namespace ReactiveExtensionsSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/sampleTime-spec.ts.
[TestFixture]
public class SampleTimeTests
{
    [Test]
    public void ShouldSampleNothingIfNewValueHasNotArrived()
    {
        var source = new Subject<int>();
        var results = new List<int>();
        using var signal = new ManualResetEventSlim();

        source.AsObservable().SampleTime(TimeSpan.FromMilliseconds(30)).Subscribe(value =>
        {
            results.Add(value);
            signal.Set();
        });

        source.OnNext(1);

        Assert.That(signal.Wait(TimeSpan.FromSeconds(2)), Is.True);
        Assert.That(results, Is.EqualTo(new[] { 1 }));
    }

    [Test]
    public void ShouldSampleOnADelayEvenIfTheSameValueIsSeenTwice()
    {
        var source = new Subject<int>();
        var results = new List<int>();
        using var signal = new ManualResetEventSlim();

        source.AsObservable().SampleTime(TimeSpan.FromMilliseconds(30)).Subscribe(value =>
        {
            results.Add(value);
            signal.Set();
        });

        source.OnNext(5);
        source.OnNext(5);

        Assert.That(signal.Wait(TimeSpan.FromSeconds(2)), Is.True);
        Assert.That(results, Is.EqualTo(new[] { 5 }));
    }

    [Test]
    public void ShouldSampleAgainOnTheNextPeriodAfterANewValueArrives()
    {
        var source = new Subject<int>();
        var results = new List<int>();
        using var firstSignal = new ManualResetEventSlim();
        using var secondSignal = new ManualResetEventSlim();

        source.AsObservable().SampleTime(TimeSpan.FromMilliseconds(30)).Subscribe(value =>
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
    public void ShouldNotEmitIfSourceHasNotNextedByTimeOfSample()
    {
        var source = new Subject<int>();
        var results = new List<int>();
        using var signal = new ManualResetEventSlim();

        source.AsObservable().SampleTime(TimeSpan.FromMilliseconds(20)).Subscribe(value =>
        {
            results.Add(value);
            signal.Set();
        });

        // No value nexted before the first period elapses: nothing should be emitted at that first tick.
        Assert.That(signal.Wait(TimeSpan.FromMilliseconds(60)), Is.False);
        Assert.That(results, Is.Empty);
    }

    [Test]
    public void ShouldRaiseErrorIfSourceRaisesError()
    {
        var source = new Subject<int>();
        var error = new InvalidOperationException("boom");
        Exception? received = null;

        source.AsObservable().SampleTime(TimeSpan.FromMilliseconds(20)).Subscribe(onError: err => received = err);

        source.OnError(error);

        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldCompleteWhenSourceCompletesWithoutFlushingAPendingValue()
    {
        var source = new Subject<int>();
        var results = new List<int>();
        var completed = false;

        source.AsObservable().SampleTime(TimeSpan.FromSeconds(5)).Subscribe(results.Add, onComplete: () => completed = true);

        source.OnNext(1);
        source.OnCompleted();

        Assert.That(results, Is.Empty, "sampleTime does not flush a pending value on completion, unlike debounce/audit");
        Assert.That(completed, Is.True);
    }

    // Regression test for the disposal-cascade fix (see CLAUDE.md Learnings): SampleTime's outer source
    // subscription must register its inner subscriber as a child of its own downstream subscriber, so a
    // disposal further down the chain (here, Take completing early) cascades all the way back and tears down
    // the source subscription. SampleTime is driven by a self-rescheduling real timer rather than a duration
    // selector, so this observes the cascade indirectly: a tracking wrapper around the source registers a
    // finalizer as a child of the subscriber SampleTime subscribes with, and asserts that finalizer runs once
    // Take(1) completes the chain. The signal is set from *inside* the finalizer itself so waiting on it can't
    // race with the cascade.
    [Test]
    public void ShouldCascadeDisposalThroughSampleTimeToTheSourceSubscription()
    {
        var source = new Subject<int>();
        using var disposalSignal = new ManualResetEventSlim();
        var sourceUnsubscribed = false;

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

        trackedSource.SampleTime(TimeSpan.FromMilliseconds(20)).Take(1).Subscribe(_ => { });

        source.OnNext(1);

        Assert.That(disposalSignal.Wait(TimeSpan.FromSeconds(2)), Is.True, "Take completing should cascade back and unsubscribe from the source");
        Assert.That(sourceUnsubscribed, Is.True);
    }
}
