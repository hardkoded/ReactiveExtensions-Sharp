using RxSharp.Operators;
using RxSharp.Subjects;

namespace RxSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/debounce-spec.ts.
[TestFixture]
public class DebounceTests
{
    [Test]
    public void ShouldEmitOnlyTheMostRecentValueAfterABurstGoesQuiet()
    {
        var source = new Subject<int>();
        var results = new List<int>();
        using var signal = new ManualResetEventSlim();

        source.AsObservable().Debounce(_ => Observable.Timer(TimeSpan.FromMilliseconds(30))).Subscribe(value =>
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

        source.AsObservable().Debounce(_ => Observable.Timer(TimeSpan.FromSeconds(5))).Subscribe(results.Add, onComplete: () => completed = true);

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

        source.AsObservable().Debounce(_ => Observable.Timer(TimeSpan.FromSeconds(5))).Subscribe(results.Add, err => received = err);

        source.OnNext(1);
        source.OnError(error);

        Assert.That(results, Is.Empty);
        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldPropagateErrorsThrownFromTheDurationSelector()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;

        Observable.Of(1).Debounce<int, Unit>(_ => throw error).Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldSupportAScalarSelectorObservableAndEmitImmediately()
    {
        var results = new List<int>();
        var completed = false;

        Observable.Of(1, 2, 3).Debounce(_ => Observable.Of(0)).Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.EqualTo(new[] { 1, 2, 3 }), "a synchronous duration selector should let every value straight through");
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldDebounceCorrectlyWhenSynchronouslyReentered()
    {
        var results = new List<int>();
        var source = new Subject<int>();

        source.AsObservable().Debounce(_ => Observable.Of(0)).Subscribe(value =>
        {
            results.Add(value);
            if (value == 1)
            {
                source.OnNext(2);
            }
        });

        source.OnNext(1);

        Assert.That(results, Is.EqualTo(new[] { 1, 2 }));
    }

    // rxjs's "should stop listening to a synchronous observable when unsubscribed" test is not ported here:
    // it relies on a `Take`-driven synchronous unsubscribe interrupting a still-running upstream `Subscribe`
    // call, which this port's `SingleAssignmentDisposable`-based teardown does not support yet (the disposal
    // only takes effect once the nested synchronous `Subscribe` call unwinds, i.e. after it already ran to
    // completion) — a pre-existing gap in `Take` itself, reproducible with `Take` alone, not specific to `Debounce`.
}
