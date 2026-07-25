using RxSharp.Operators;
using RxSharp.Subjects;

namespace RxSharp.Tests.Operators;

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
}
