using RxSharp.Operators;

namespace RxSharp.Tests.Operators;

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

    // rxjs's "should stop listening to a synchronous observable when unsubscribed" test (repeat() + take(3)
    // atop a hand-rolled loop-based source) is not ported here: Repeat sits as an intermediate operator between
    // the raw source and Take, and this port's disposal-linking only takes effect once the nested synchronous
    // Subscribe call unwinds — i.e. after the loop already ran to completion. This is the same pre-existing,
    // documented gap noted in AuditTests.cs/DebounceTests.cs/ThrottleTests.cs/WindowCountTests.cs, reproducible
    // with Take alone (see CLAUDE.md's Learnings), not specific to Repeat.
}
