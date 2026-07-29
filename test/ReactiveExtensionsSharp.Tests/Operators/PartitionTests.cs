using ReactiveExtensionsSharp.Operators;

namespace ReactiveExtensionsSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/observables/partition-spec.ts. rxjs's own partition is
// literally `[filter(predicate)(source), filter(not(predicate))(source)]` — two independent subscriptions to
// the source, one per returned observable — confirmed against
// src/internal/observable/partition.ts at the 7.8.2 tag. These tests port that behavior rather than the
// marble/expectSubscriptions assertions (no subscription-log helper exists yet in ReactiveExtensionsSharp.Testing), using
// plain result-list/subscription-count assertions instead, in the same style as FilterTests/GroupByTests.
[TestFixture]
public class PartitionTests
{
    [Test]
    public void ShouldPartitionAnObservableOfIntegersIntoEvenAndOdd()
    {
        var (odds, evens) = Observable.Of(1, 2, 3, 4, 5, 6).Partition(x => x % 2 == 1);

        var oddResults = new List<int>();
        var evenResults = new List<int>();
        odds.Subscribe(oddResults.Add);
        evens.Subscribe(evenResults.Add);

        Assert.That(oddResults, Is.EqualTo(new[] { 1, 3, 5 }));
        Assert.That(evenResults, Is.EqualTo(new[] { 2, 4, 6 }));
    }

    [Test]
    public void ShouldPartitionUsingAPredicate()
    {
        var (matched, unmatched) = Observable.Of("a", "b", "a", "d", "a", "c").Partition(x => x == "a");

        var matchedResults = new List<string>();
        var unmatchedResults = new List<string>();
        matched.Subscribe(matchedResults.Add);
        unmatched.Subscribe(unmatchedResults.Add);

        Assert.That(matchedResults, Is.EqualTo(new[] { "a", "a", "a" }));
        Assert.That(unmatchedResults, Is.EqualTo(new[] { "b", "d", "c" }));
    }

    [Test]
    public void ShouldPartitionUsingAPredicateThatTakesAnIndex()
    {
        var (matched, unmatched) = Observable.Of("a", "b", "a", "d", "a", "c").Partition((_, index) => index % 2 == 0);

        var matchedResults = new List<string>();
        var unmatchedResults = new List<string>();
        matched.Subscribe(matchedResults.Add);
        unmatched.Subscribe(unmatchedResults.Add);

        Assert.That(matchedResults, Is.EqualTo(new[] { "a", "a", "a" }));
        Assert.That(unmatchedResults, Is.EqualTo(new[] { "b", "d", "c" }));
    }

    [Test]
    public void ShouldPassErrorsToBothReturnedObservables()
    {
        var error = new InvalidOperationException("source error");
        var (matched, unmatched) = Observable.ThrowError<string>(() => error).Partition(x => x == "a");

        Exception? matchedError = null;
        Exception? unmatchedError = null;
        matched.Subscribe(onError: err => matchedError = err);
        unmatched.Subscribe(onError: err => unmatchedError = err);

        Assert.That(matchedError, Is.SameAs(error));
        Assert.That(unmatchedError, Is.SameAs(error));
    }

    [Test]
    public void ShouldPassErrorsToBothReturnedObservablesIfPredicateThrows()
    {
        var error = new InvalidOperationException("predicate error");
        var (matched, unmatched) = Observable.Of("a", "b", "a").Partition<string>(_ => throw error);

        Exception? matchedError = null;
        Exception? unmatchedError = null;
        matched.Subscribe(onError: err => matchedError = err);
        unmatched.Subscribe(onError: err => unmatchedError = err);

        Assert.That(matchedError, Is.SameAs(error));
        Assert.That(unmatchedError, Is.SameAs(error));
    }

    [Test]
    public void ShouldPartitionAnEmptyObservable()
    {
        var (matched, unmatched) = Observable.Empty<string>().Partition(x => x == "x");

        var matchedCompleted = false;
        var unmatchedCompleted = false;
        matched.Subscribe(onComplete: () => matchedCompleted = true);
        unmatched.Subscribe(onComplete: () => unmatchedCompleted = true);

        Assert.That(matchedCompleted, Is.True);
        Assert.That(unmatchedCompleted, Is.True);
    }

    [Test]
    public void ShouldPartitionIfSourceEmitsASingleElement()
    {
        var (matched, unmatched) = Observable.Of("a").Partition(x => x == "a");

        var matchedResults = new List<string>();
        var unmatchedResults = new List<string>();
        matched.Subscribe(matchedResults.Add);
        unmatched.Subscribe(unmatchedResults.Add);

        Assert.That(matchedResults, Is.EqualTo(new[] { "a" }));
        Assert.That(unmatchedResults, Is.Empty);
    }

    [Test]
    public void ShouldPartitionIfThePredicateMatchesAllOfSourceElements()
    {
        var (matched, unmatched) = Observable.Of("a", "a", "a").Partition(x => x == "a");

        var matchedResults = new List<string>();
        var unmatchedResults = new List<string>();
        matched.Subscribe(matchedResults.Add);
        unmatched.Subscribe(unmatchedResults.Add);

        Assert.That(matchedResults, Is.EqualTo(new[] { "a", "a", "a" }));
        Assert.That(unmatchedResults, Is.Empty);
    }

    // rxjs asserts this via `expectSubscriptions(e1.subscriptions).toBe([e1subs, e1subs])` — the source gets
    // exactly two independent subscriptions, one per returned observable. ReactiveExtensionsSharp.Testing has no
    // subscription-log helper yet, so this counts subscriptions directly against a hand-rolled source instead.
    [Test]
    public void ShouldSubscribeToTheSourceExactlyOncePerReturnedObservable()
    {
        var subscribeCount = 0;
        var source = new Observable<int>(subscriber =>
        {
            subscribeCount++;
            subscriber.OnNext(1);
            subscriber.OnNext(2);
            subscriber.OnCompleted();
        });

        var (matched, unmatched) = source.Partition(x => x == 1);
        matched.Subscribe();
        unmatched.Subscribe();

        Assert.That(subscribeCount, Is.EqualTo(2));
    }

    // Confirms the two returned observables have fully independent subscriptions to the source: disposing one
    // side's subscription must not affect the other side's, since each is its own filter over its own
    // subscription (matches rxjs's literal two-independent-`filter`-pipes implementation).
    [Test]
    public void ShouldKeepTheTwoSubscriptionsIndependent()
    {
        var activeSubscriptions = 0;
        var source = new Observable<int>(subscriber =>
        {
            activeSubscriptions++;
            return new AnonymousDisposable(() => activeSubscriptions--);
        });

        var (matched, unmatched) = source.Partition(x => x == 1);
        var matchedSubscription = matched.Subscribe();
        var unmatchedSubscription = unmatched.Subscribe();

        Assert.That(activeSubscriptions, Is.EqualTo(2));

        matchedSubscription.Dispose();
        Assert.That(activeSubscriptions, Is.EqualTo(1), "disposing one side must not tear down the other side's independent subscription");

        unmatchedSubscription.Dispose();
        Assert.That(activeSubscriptions, Is.EqualTo(0));
    }

    private sealed class AnonymousDisposable : IDisposable
    {
        private readonly Action _dispose;

        public AnonymousDisposable(Action dispose)
            => _dispose = dispose;

        public void Dispose()
            => _dispose();
    }
}
