using RxSharp.Operators;
using RxSharp.Subjects;

namespace RxSharp.Tests.Operators;

// Ported from rxjs 7.8.2 spec/operators/takeUntil-spec.ts (non-marble cases).
[TestFixture]
public class TakeUntilTests
{
    [Test]
    public void ShouldTakeValuesUntilNotifierEmits()
    {
        var source = new Subject<int>();
        var notifier = new Subject<int>();
        var results = new List<int>();
        var completed = false;

        source.AsObservable().TakeUntil(notifier.AsObservable()).Subscribe(results.Add, onComplete: () => completed = true);

        source.OnNext(1);
        source.OnNext(2);
        notifier.OnNext(0);
        source.OnNext(3);

        Assert.That(results, Is.EqualTo(new[] { 1, 2 }));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldPassAllValuesIfNotifierNeverEmits()
    {
        var results = new List<int>();
        Observable.Of(1, 2, 3).TakeUntil(Observable.Never<int>()).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 1, 2, 3 }));
    }

    [Test]
    public void ShouldCompleteImmediatelyIfNotifierEmitsSynchronouslyBeforeSourceSubscription()
    {
        var results = new List<int>();
        var completed = false;
        Observable.Of(1, 2, 3).TakeUntil(Observable.Of(0)).Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.Empty);
        Assert.That(completed, Is.True);
    }

    // Corrected from an earlier draft of this test that (incorrectly) expected a notifier error to propagate to
    // the output. Real rxjs's takeUntil.ts passes an explicit `noop` as the notifier's error handler
    // (createOperatorSubscriber(subscriber, () => subscriber.complete(), noop)) -- a notifier error has no
    // effect on the output at all, matching this operator's own XML doc remarks. Verified against the 7.8.2 tag
    // rather than assumed.
    [Test]
    public void ShouldIgnoreAnErrorFromTheNotifierAndKeepMirroringTheSource()
    {
        var source = new Subject<int>();
        var notifier = new Subject<int>();
        var results = new List<int>();
        var errored = false;
        var completed = false;

        source.AsObservable().TakeUntil(notifier.AsObservable()).Subscribe(results.Add, onError: _ => errored = true, onComplete: () => completed = true);

        source.OnNext(1);
        notifier.OnError(new InvalidOperationException("boom"));
        source.OnNext(2);
        source.OnCompleted();

        Assert.That(results, Is.EqualTo(new[] { 1, 2 }));
        Assert.That(errored, Is.False);
        Assert.That(completed, Is.True);
    }

    // Ported from rxjs's "should dispose notifier if source observable completes" test: once the source
    // completes on its own (before the notifier ever emits), the notifier subscription must be torn down too.
    [Test]
    public void ShouldDisposeTheNotifierWhenSourceCompletesBeforeNotifierEmits()
    {
        var notifierDisposed = false;
        var notifier = new Observable<int>(_ => new Subscription(() => notifierDisposed = true));
        var results = new List<int>();
        var completed = false;

        Observable.Of(1, 2).TakeUntil(notifier).Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.EqualTo(new[] { 1, 2 }));
        Assert.That(completed, Is.True);
        Assert.That(notifierDisposed, Is.True);
    }
}
