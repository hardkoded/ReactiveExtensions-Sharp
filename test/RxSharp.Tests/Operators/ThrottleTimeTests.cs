using RxSharp.Operators;
using RxSharp.Subjects;

namespace RxSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/throttleTime-spec.ts.
[TestFixture]
public class ThrottleTimeTests
{
    [Test]
    public void DefaultBehaviorShouldImmediatelyEmitTheFirstValueInEachTimeWindow()
    {
        var source = new Subject<int>();
        var results = new List<int>();

        source.AsObservable().ThrottleTime(TimeSpan.FromMilliseconds(30)).Subscribe(results.Add);

        source.OnNext(1);
        source.OnNext(2);
        source.OnNext(3);

        Assert.That(results, Is.EqualTo(new[] { 1 }));
    }

    [Test]
    public void ShouldThrottleEventsAndOnlyEmitTheFirstOne()
    {
        var results = new List<int>();

        Observable.Of(1, 2, 3).ThrottleTime(TimeSpan.FromMilliseconds(5)).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 1 }));
    }

    [Test]
    public void ShouldSimplyMirrorTheSourceIfValuesAreNotEmittedOftenEnough()
    {
        var source = new Subject<int>();
        var results = new List<int>();
        using var signal = new ManualResetEventSlim();

        source.AsObservable().ThrottleTime(TimeSpan.FromMilliseconds(20)).Subscribe(value =>
        {
            results.Add(value);
            if (results.Count == 2)
            {
                signal.Set();
            }
        });

        source.OnNext(1);
        Thread.Sleep(60); // well past the throttle window, so the second value should not be dropped
        source.OnNext(2);

        Assert.That(signal.Wait(TimeSpan.FromSeconds(2)), Is.True);
        Assert.That(results, Is.EqualTo(new[] { 1, 2 }));
    }

    [Test]
    public void WithTrailingEnabledShouldEmitTheFirstAndLastValuesInEachTimeWindow()
    {
        var source = new Subject<int>();
        var results = new List<int>();
        using var signal = new ManualResetEventSlim();

        source.AsObservable().ThrottleTime(TimeSpan.FromMilliseconds(30), leading: true, trailing: true).Subscribe(value =>
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
        Assert.That(results, Is.EqualTo(new[] { 1, 3 }));
    }

    [Test]
    public void ShouldCompleteWhenSourceDoesNotEmit()
    {
        var source = new Subject<int>();
        var results = new List<int>();
        var completed = false;

        source.AsObservable().ThrottleTime(TimeSpan.FromMilliseconds(20)).Subscribe(results.Add, onComplete: () => completed = true);

        source.OnCompleted();

        Assert.That(results, Is.Empty);
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldRaiseErrorWhenSourceRaisesError()
    {
        var source = new Subject<int>();
        var error = new InvalidOperationException("boom");
        Exception? received = null;

        source.AsObservable().ThrottleTime(TimeSpan.FromMilliseconds(20)).Subscribe(onError: err => received = err);

        source.OnError(error);

        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void WithTrailingEnabledShouldEmitTheTrailingValueOnceMoreAfterSourceCompletes()
    {
        var source = new Subject<int>();
        var results = new List<int>();
        var completed = false;
        using var signal = new ManualResetEventSlim();

        source.AsObservable().ThrottleTime(TimeSpan.FromMilliseconds(30), leading: true, trailing: true).Subscribe(
            results.Add,
            onComplete: () =>
            {
                completed = true;
                signal.Set();
            });

        source.OnNext(1);
        source.OnNext(2);
        source.OnCompleted();

        Assert.That(signal.Wait(TimeSpan.FromSeconds(2)), Is.True);
        Assert.That(results, Is.EqualTo(new[] { 1, 2 }), "the trailing value should still flush even though the source already completed");
        Assert.That(completed, Is.True);
    }
}
