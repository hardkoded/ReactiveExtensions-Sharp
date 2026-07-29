using ReactiveExtensionsSharp.Operators;
using ReactiveExtensionsSharp.Subjects;

namespace ReactiveExtensionsSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/sample-spec.ts.
[TestFixture]
public class SampleTests
{
    [Test]
    public void ShouldGetSamplesWhenTheNotifierEmits()
    {
        var source = new Subject<int>();
        var notifier = new Subject<Unit>();
        var results = new List<int>();

        source.AsObservable().Sample(notifier.AsObservable()).Subscribe(results.Add);

        source.OnNext(1);
        notifier.OnNext(Unit.Default);
        Assert.That(results, Is.EqualTo(new[] { 1 }));

        notifier.OnNext(Unit.Default);
        Assert.That(results, Is.EqualTo(new[] { 1 }), "a notifier tick with no new value since the last sample should not emit");

        source.OnNext(2);
        source.OnNext(3);
        notifier.OnNext(Unit.Default);
        Assert.That(results, Is.EqualTo(new[] { 1, 3 }), "only the most recent value should be emitted");
    }

    [Test]
    public void ShouldSampleNothingIfSourceHasNotNextedAtAll()
    {
        var source = new Subject<int>();
        var notifier = new Subject<Unit>();
        var results = new List<int>();

        source.AsObservable().Sample(notifier.AsObservable()).Subscribe(results.Add);

        notifier.OnNext(Unit.Default);

        Assert.That(results, Is.Empty);
    }

    [Test]
    public void ShouldBehaveProperlyWhenNotifiedByTheSameObservableAsTheSource()
    {
        var item = new Subject<int>();
        var results = new List<int>();

        item.AsObservable().Sample(item.AsObservable()).Subscribe(results.Add);

        item.OnNext(1);
        item.OnNext(2);
        item.OnNext(3);

        Assert.That(results, Is.EqualTo(new[] { 1, 2, 3 }));
    }

    [Test]
    public void ShouldNotSampleOrCompleteWhenTheNotifierCompletes()
    {
        var source = new Subject<int>();
        var notifier = new Subject<Unit>();
        var results = new List<int>();
        var completed = false;

        source.AsObservable().Sample(notifier.AsObservable()).Subscribe(results.Add, onComplete: () => completed = true);

        notifier.OnCompleted();
        source.OnNext(1);

        Assert.That(results, Is.Empty);
        Assert.That(completed, Is.False);
    }

    [Test]
    public void ShouldRaiseErrorIfSourceRaisesError()
    {
        var source = new Subject<int>();
        var notifier = new Subject<Unit>();
        var error = new InvalidOperationException("boom");
        Exception? received = null;

        source.AsObservable().Sample(notifier.AsObservable()).Subscribe(onError: err => received = err);

        source.OnError(error);

        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldRaiseErrorIfNotifierRaisesError()
    {
        var source = new Subject<int>();
        var notifier = new Subject<Unit>();
        var error = new InvalidOperationException("boom");
        Exception? received = null;

        source.AsObservable().Sample(notifier.AsObservable()).Subscribe(onError: err => received = err);

        source.OnNext(1);
        notifier.OnError(error);

        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldCompleteOnlyWhenTheSourceCompletes()
    {
        var source = new Subject<int>();
        var notifier = new Subject<Unit>();
        var completed = false;

        source.AsObservable().Sample(notifier.AsObservable()).Subscribe(onComplete: () => completed = true);

        notifier.OnCompleted();
        Assert.That(completed, Is.False);

        source.OnCompleted();
        Assert.That(completed, Is.True);
    }

    // Regression test for the disposal-cascade fix (see CLAUDE.md Learnings): Sample's outer subscriptions (to
    // both the source and the notifier) must register their inner subscribers as children of Sample's own
    // downstream subscriber, so a disposal further down the chain (here, Take completing early) cascades all the
    // way back to a fully synchronous, self-checking notifier mid-loop.
    //
    // Sample subscribes to the source before the notifier, so the self-checking loop has to live on the
    // notifier side, not the source side: a self-checking *source* would run to completion in one synchronous
    // burst before the notifier (which is what actually triggers forwarding) is even subscribed, so no downstream
    // disposal could ever reach it mid-loop. `StartWith(0)` seeds an initial value synchronously as Sample
    // subscribes to the source, and the final subscriber reentrantly pushes a fresh source value on every
    // forwarded sample, so each notifier tick has something new to sample.
    [Test]
    public void ShouldCascadeDisposalThroughSampleToASynchronousNotifier()
    {
        var sideEffects = new List<int>();
        var source = new Subject<int>();
        var next = 1;

        Observable<Unit> notifier = new(subscriber =>
        {
            for (var i = 0; !subscriber.IsDisposed && i < 10; i++)
            {
                sideEffects.Add(i);
                subscriber.OnNext(Unit.Default);
            }
        });

        source.AsObservable().StartWith(0)
            .Sample(notifier)
            .Take(3)
            .Subscribe(_ => source.OnNext(next++));

        Assert.That(sideEffects, Is.EqualTo(new[] { 0, 1, 2 }));
    }
}
