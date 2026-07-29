using ReactiveExtensionsSharp.Operators;
using ReactiveExtensionsSharp.Subjects;

namespace ReactiveExtensionsSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/repeat-spec.ts.
[TestFixture]
public class RepeatTests
{
    [Test]
    public void ShouldResubscribeCountNumberOfTimes()
    {
        var results = new List<string>();
        var completed = false;
        Observable.Of("a", "b").Repeat(3).Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.EqualTo(new[] { "a", "b", "a", "b", "a", "b" }));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldEmitSourceOnceWhenCountIsOne()
    {
        var results = new List<string>();
        Observable.Of("a", "b").Repeat(1).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { "a", "b" }));
    }

    [Test]
    public void ShouldCompleteWithoutSubscribingWhenCountIsZero()
    {
        var subscribed = false;
        var completed = false;
        var results = new List<int>();
        new Observable<int>(_ => subscribed = true).Repeat(0).Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(subscribed, Is.False);
        Assert.That(results, Is.Empty);
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldCompleteWithoutSubscribingWhenCountIsNegative()
    {
        var subscribed = false;
        var completed = false;
        new Observable<int>(_ => subscribed = true).Repeat(-1).Subscribe(onComplete: () => completed = true);

        Assert.That(subscribed, Is.False);
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldRepeatIndefinitelyUntilDownstreamStopsListening()
    {
        var results = new List<string>();
        Observable.Of("a", "b").Repeat().Take(5).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { "a", "b", "a", "b", "a" }));
    }

    [Test]
    public void ShouldNotRetryOnErrorAndForwardItImmediately()
    {
        var error = new InvalidOperationException("boom");
        var results = new List<string>();
        Exception? received = null;

        new Observable<string>(subscriber =>
        {
            subscriber.OnNext("a");
            subscriber.OnNext("b");
            subscriber.OnError(error);
        }).Repeat(3).Subscribe(results.Add, onError: err => received = err);

        Assert.That(results, Is.EqualTo(new[] { "a", "b" }));
        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldNotCompleteWhenSourceNeverCompletes()
    {
        var completed = false;
        Observable.Never<int>().Repeat(3).Subscribe(onComplete: () => completed = true);

        Assert.That(completed, Is.False);
    }

    [Test]
    public void ShouldAlwaysFinalizeBeforeStartingTheNextCycleEvenWhenSynchronous()
    {
        // The source registers its own teardown via subscriber.Add(...) (this port's idiom for a source with
        // finalization logic -- see Observable's Action<Subscriber<T>> constructor) rather than rxjs's
        // return-a-teardown-function style, which isn't meaningfully portable here: rxjs's teardown function
        // is returned from the subscribe callback itself, but this source calls OnCompleted() *before*
        // returning, at which point rxjs's equivalent teardown wouldn't exist as a value yet either.
        var results = new List<object>();
        var source = new Observable<int>(subscriber =>
        {
            subscriber.Add(() => results.Add("finalizer"));
            subscriber.OnNext(1);
            subscriber.OnNext(2);
            subscriber.OnCompleted();
        });

        var completed = false;
        source.Repeat(3).Subscribe(value => results.Add(value), onComplete: () => completed = true);

        // Each cycle's finalizer must run before the next cycle's values start arriving -- not merely
        // "eventually", which is all a fully recursive (call-and-return-unwind-ordered) resubscription would
        // guarantee for a fully synchronous source.
        Assert.That(results, Is.EqualTo(new object[] { 1, 2, "finalizer", 1, 2, "finalizer", 1, 2, "finalizer" }));
        Assert.That(completed, Is.True);
    }

    // rxjs's "should stop listening to a synchronous observable when unsubscribed" test, ported now that the
    // M6 disposal-cascade fix (see CLAUDE.md's Learnings) applies here too: Repeat's per-cycle cycleSubscriber
    // is registered as a child of the downstream subscriber before subscribing, so a downstream Take completing
    // early correctly cascades up and stops a fully-synchronous, self-checking source mid-loop.
    [Test]
    public void ShouldStopListeningToASynchronousObservableWhenUnsubscribed()
    {
        var sideEffects = new List<int>();
        var synchronousObservable = new Observable<int>(subscriber =>
        {
            for (var i = 0; !subscriber.IsDisposed && i < 10; i++)
            {
                sideEffects.Add(i);
                subscriber.OnNext(i);
            }
        });

        synchronousObservable.Repeat().Take(3).Subscribe(_ => { });

        Assert.That(sideEffects, Is.EqualTo(new[] { 0, 1, 2 }));
    }

    [Test]
    public void ShouldRepeatUsingANotifierSelectorDelay()
    {
        var results = new List<string>();
        var completed = false;
        var cycleCount = 0;
        var delaysRequested = new List<int>();
        var notifier = new Subject<Unit>();

        Observable.Of("a", "b").Repeat<string, Unit>(cycle =>
        {
            cycleCount++;
            delaysRequested.Add(cycle);
            return notifier.AsObservable();
        }, count: 3).Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(delaysRequested, Is.EqualTo(new[] { 1 }));
        Assert.That(results, Is.EqualTo(new[] { "a", "b" }), "should wait for the notifier before repeating");

        notifier.OnNext(Unit.Default);

        Assert.That(delaysRequested, Is.EqualTo(new[] { 1, 2 }));
        Assert.That(results, Is.EqualTo(new[] { "a", "b", "a", "b" }));
        Assert.That(completed, Is.False);

        notifier.OnNext(Unit.Default);

        Assert.That(results, Is.EqualTo(new[] { "a", "b", "a", "b", "a", "b" }));
        Assert.That(completed, Is.True, "count of 3 total cycles has been reached, no third delay should be requested");
        Assert.That(cycleCount, Is.EqualTo(2));
    }

    [Test]
    public void ShouldPropagateErrorThrownFromTheRepeatDelaySelectorFunction()
    {
        var thrown = new InvalidOperationException("selector boom");
        Exception? received = null;

        Observable.Of(1).Repeat<int, Unit>(_ => throw thrown).Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(thrown));
    }
}
