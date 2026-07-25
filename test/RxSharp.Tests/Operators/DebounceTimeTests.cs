using RxSharp.Operators;
using RxSharp.Subjects;

namespace RxSharp.Tests.Operators;

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
}
