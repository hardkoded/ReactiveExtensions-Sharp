using System.Globalization;
using ReactiveExtensionsSharp.Operators;

namespace ReactiveExtensionsSharp.Tests;

[TestFixture]
public class ObservableCreationTests
{
    [Test]
    public void Timer_ShouldEmitZeroThenCompleteAfterTheDelay()
    {
        using var signal = new ManualResetEventSlim();
        var results = new List<long>();
        var completed = false;

        Observable.Timer(TimeSpan.FromMilliseconds(20)).Subscribe(
            results.Add,
            onComplete: () =>
            {
                completed = true;
                signal.Set();
            });

        Assert.That(signal.Wait(TimeSpan.FromSeconds(2)), Is.True);
        Assert.That(results, Is.EqualTo(new[] { 0L }));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void Timer_ShouldBeCancellableBeforeItFires()
    {
        var fired = false;
        var subscription = Observable.Timer(TimeSpan.FromMilliseconds(30)).Subscribe(_ => fired = true);
        subscription.Dispose();

        Thread.Sleep(80);

        Assert.That(fired, Is.False);
    }

    [Test]
    public void Race_ShouldMirrorTheFirstSourceToEmit()
    {
        using var signal = new ManualResetEventSlim();
        var results = new List<string>();

        var slow = Observable.Timer(TimeSpan.FromMilliseconds(80)).Map(_ => "slow");
        var fast = Observable.Timer(TimeSpan.FromMilliseconds(10)).Map(_ => "fast");

        Observable.Race(slow, fast).Subscribe(results.Add, onComplete: () => signal.Set());

        Assert.That(signal.Wait(TimeSpan.FromSeconds(2)), Is.True);
        Assert.That(results, Is.EqualTo(new[] { "fast" }));
    }

    [Test]
    public void Race_ShouldUnsubscribeTheLosers()
    {
        using var signal = new ManualResetEventSlim();
        var loserFired = false;

        var loser = Observable.Timer(TimeSpan.FromMilliseconds(100)).Map(_ => { loserFired = true; return "slow"; });
        var winner = Observable.Timer(TimeSpan.FromMilliseconds(10)).Map(_ => "fast");

        Observable.Race(loser, winner).Subscribe(onComplete: () => signal.Set());
        Assert.That(signal.Wait(TimeSpan.FromSeconds(2)), Is.True);

        Thread.Sleep(150);

        Assert.That(loserFired, Is.False);
    }

    [Test]
    public void Concat_ShouldEmitEachSourceInSequenceAfterTheOneBeforeCompletes()
    {
        var results = new List<int>();
        var completed = false;
        Observable.Concat(Observable.Of(1, 2), Observable.Of(3, 4)).Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.EqualTo(new[] { 1, 2, 3, 4 }));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void Concat_ShouldNeverSubscribeToLaterSourcesIfAnEarlierOneErrors()
    {
        var error = new InvalidOperationException("boom");
        var secondSubscribed = false;
        Exception? received = null;

        Observable.Concat(
                Observable.ThrowError<int>(() => error),
                new Observable<int>(subscriber =>
                {
                    secondSubscribed = true;
                    subscriber.OnCompleted();
                }))
            .Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
        Assert.That(secondSubscribed, Is.False);
    }

    // Ported (non-marble subset) from rxjs 7.8.2 spec/observables/onErrorResumeNext-spec.ts.
    [Test]
    public void OnErrorResumeNext_ShouldMoveToTheNextSourceOnErrorOrOnComplete()
    {
        var results = new List<int>();
        var completed = false;

        Observable.OnErrorResumeNext(
                Observable.Concat(Observable.Of(1, 2), Observable.ThrowError<int>(() => new InvalidOperationException("s1"))),
                Observable.Concat(Observable.Of(3, 4), Observable.ThrowError<int>(() => new InvalidOperationException("s2"))),
                Observable.Of(5, 6),
                Observable.Of(7))
            .Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.EqualTo(new[] { 1, 2, 3, 4, 5, 6, 7 }));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void OnErrorResumeNext_ShouldCompleteImmediatelyWhenGivenNoSources()
    {
        var completed = false;
        Observable.OnErrorResumeNext<int>().Subscribe(onComplete: () => completed = true);

        Assert.That(completed, Is.True);
    }

    [Test]
    public void OnErrorResumeNext_ShouldCompleteRatherThanErrorWhenTheOnlySourceErrors()
    {
        var completed = false;
        var errored = false;

        Observable.OnErrorResumeNext(Observable.ThrowError<int>(() => new InvalidOperationException("boom")))
            .Subscribe(onError: _ => errored = true, onComplete: () => completed = true);

        Assert.That(errored, Is.False);
        Assert.That(completed, Is.True);
    }

    [Test]
    public void OnErrorResumeNext_ShouldRunEachSourcesOwnTeardownBeforeSubscribingToTheNext()
    {
        var results = new List<object>();

        Observable<int> WithTeardown(int value, string label) => new Observable<int>(subscriber =>
        {
            subscriber.Add(() => results.Add(label));
            subscriber.OnNext(value);
            subscriber.OnCompleted();
        });

        Observable.OnErrorResumeNext(
                WithTeardown(1, "finalize 1"),
                WithTeardown(2, "finalize 2"),
                WithTeardown(3, "finalize 3"),
                WithTeardown(4, "finalize 4"))
            .Subscribe(v => results.Add(v), onComplete: () => results.Add("complete"));

        Assert.That(
            results,
            Is.EqualTo(new object[] { 1, "finalize 1", 2, "finalize 2", 3, "finalize 3", 4, "finalize 4", "complete" }));
    }

    [Test]
    public void OnErrorResumeNext_ShouldNotAdvanceToTheNextSourceWhenExternallyUnsubscribed()
    {
        var secondSubscribed = false;
        var subscription = Observable.OnErrorResumeNext(
            Observable.Never<int>(),
            new Observable<int>(subscriber =>
            {
                secondSubscribed = true;
                subscriber.OnCompleted();
            })).Subscribe();

        subscription.Dispose();

        Assert.That(secondSubscribed, Is.False, "an external unsubscribe must stop the chain, not advance it");
    }

    [Test]
    public void Merge_ShouldEmitFromAllSourcesConcurrentlyAndCompleteWhenAllHaveCompleted()
    {
        var subjectA = new ReactiveExtensionsSharp.Subjects.Subject<int>();
        var subjectB = new ReactiveExtensionsSharp.Subjects.Subject<int>();
        var results = new List<int>();
        var completed = false;

        Observable.Merge(subjectA.AsObservable(), subjectB.AsObservable()).Subscribe(results.Add, onComplete: () => completed = true);

        subjectA.OnNext(1);
        subjectB.OnNext(2);
        subjectA.OnNext(3);
        subjectA.OnCompleted();
        Assert.That(completed, Is.False);

        subjectB.OnCompleted();

        Assert.That(results, Is.EqualTo(new[] { 1, 2, 3 }));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void FromEvent_ShouldForwardEventArgsAsValuesAndRemoveTheHandlerOnUnsubscribe()
    {
        var publisher = new TestEventPublisher();
        var results = new List<int>();

        var subscription = Observable.FromEvent<int>(h => publisher.Changed += h, h => publisher.Changed -= h).Subscribe(results.Add);

        publisher.RaiseChanged(1);
        publisher.RaiseChanged(2);
        subscription.Dispose();
        publisher.RaiseChanged(3);

        Assert.That(results, Is.EqualTo(new[] { 1, 2 }));
        Assert.That(publisher.HasSubscribers, Is.False);
    }

    [Test]
    public void Zip_ShouldCombinePositionallyAndCompleteWhenTheShortestSourceIsExhausted()
    {
        var results = new List<IReadOnlyList<int>>();
        var completed = false;
        Observable.Zip(Observable.Of(1, 2, 3), Observable.Of(10, 20)).Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results.Select(r => r.ToArray()), Is.EqualTo(new[] { new[] { 1, 10 }, new[] { 2, 20 } }));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ForkJoin_ShouldEmitTheLastValueOfEachSourceOnceAllHaveCompleted()
    {
        var subjectA = new ReactiveExtensionsSharp.Subjects.Subject<int>();
        var subjectB = new ReactiveExtensionsSharp.Subjects.Subject<int>();
        var results = new List<IReadOnlyList<int>>();

        Observable.ForkJoin(subjectA.AsObservable(), subjectB.AsObservable()).Subscribe(results.Add);

        subjectA.OnNext(1);
        subjectA.OnNext(2);
        subjectA.OnCompleted();
        Assert.That(results, Is.Empty, "shouldn't emit until every source has completed");

        subjectB.OnNext(10);
        subjectB.OnCompleted();

        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0], Is.EqualTo(new[] { 2, 10 }));
    }

    [Test]
    public void ForkJoin_ShouldCompleteWithoutEmittingIfAnySourceNeverEmitsAValue()
    {
        var results = new List<IReadOnlyList<int>>();
        var completed = false;
        Observable.ForkJoin(Observable.Of(1), Observable.Empty<int>()).Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.Empty);
        Assert.That(completed, Is.True);
    }

    [Test]
    public void CombineLatest_ShouldEmitOnEveryEmissionOnceAllSourcesHaveEmittedAtLeastOnce()
    {
        var subjectA = new ReactiveExtensionsSharp.Subjects.Subject<int>();
        var subjectB = new ReactiveExtensionsSharp.Subjects.Subject<string>();
        var results = new List<IReadOnlyList<object>>();

        Observable.CombineLatest<object>(subjectA.AsObservable().Map(x => (object)x), subjectB.AsObservable().Map(x => (object)x))
            .Subscribe(results.Add);

        subjectA.OnNext(1);
        Assert.That(results, Is.Empty, "B hasn't emitted yet");

        subjectB.OnNext("x");
        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0], Is.EqualTo(new object[] { 1, "x" }));

        subjectA.OnNext(2);
        Assert.That(results[1], Is.EqualTo(new object[] { 2, "x" }));
    }

    // Ported (non-marble subset) from rxjs 7.8.2 spec/observables/interval-spec.ts.
    [Test]
    public void Interval_ShouldEmitIncrementingValuesUntilUnsubscribed()
    {
        using var signal = new ManualResetEventSlim();
        var results = new List<long>();

        var subscription = Observable.Interval(TimeSpan.FromMilliseconds(15)).Subscribe(value =>
        {
            results.Add(value);
            if (value == 3)
            {
                signal.Set();
            }
        });

        Assert.That(signal.Wait(TimeSpan.FromSeconds(2)), Is.True);
        subscription.Dispose();

        Assert.That(results, Is.EqualTo(new[] { 0L, 1L, 2L, 3L }));
    }

    [Test]
    public void Interval_ShouldStopTickingOnceUnsubscribed()
    {
        var count = 0;
        var subscription = Observable.Interval(TimeSpan.FromMilliseconds(15)).Subscribe(_ => count++);

        Thread.Sleep(40);
        subscription.Dispose();
        var countAfterDispose = count;

        Thread.Sleep(80);

        Assert.That(count, Is.EqualTo(countAfterDispose), "no further ticks should have fired after unsubscribing");
    }

    // Ported (non-marble subset) from rxjs 7.8.2 spec/observables/range-spec.ts.
    [Test]
    public void Range_ShouldSynchronouslyEmitTheGivenCountStartingAtStart()
    {
        var results = new List<int>();
        var completed = false;
        Observable.Range(12, 4).Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.EqualTo(new[] { 12, 13, 14, 15 }));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void Range_SingleArgOverload_ShouldStartAtZero()
    {
        var results = new List<int>();
        Observable.Range(5).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 0, 1, 2, 3, 4 }));
    }

    [Test]
    public void Range_ShouldReturnEmptyForZeroCount()
    {
        var results = new List<object>();
        Observable.Range(0).Subscribe(v => results.Add(v), onComplete: () => results.Add("done"));

        Assert.That(results, Is.EqualTo(new object[] { "done" }));
    }

    [Test]
    public void Range_ShouldReturnEmptyForANegativeCount()
    {
        var results = new List<object>();
        Observable.Range(5, -5).Subscribe(v => results.Add(v), onComplete: () => results.Add("done"));

        Assert.That(results, Is.EqualTo(new object[] { "done" }));
    }

    // Ported (non-marble subset) from rxjs 7.8.2 spec/observables/generate-spec.ts.
    [Test]
    public void Generate_ShouldCompleteIfConditionDoesNotHold()
    {
        var results = new List<int>();
        var completed = false;
        Observable.Generate(1, x => false, x => x + 1).Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.Empty);
        Assert.That(completed, Is.True);
    }

    [Test]
    public void Generate_ShouldProduceAllValuesSynchronously()
    {
        var results = new List<int>();
        Observable.Generate(1, x => x < 3, x => x + 1).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 1, 2 }));
    }

    [Test]
    public void Generate_ShouldUseTheResultSelector()
    {
        var results = new List<string>();
        Observable.Generate(1, x => x < 3, x => x + 1, x => (x + 1).ToString(CultureInfo.InvariantCulture)).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { "2", "3" }));
    }

    [Test]
    public void Generate_ShouldAllowOmittingTheCondition()
    {
        // No dedicated "no condition, with resultSelector" overload exists (see the XML doc on the
        // no-condition/no-resultSelector overload for why) -- pass an always-true condition explicitly instead.
        var results = new List<string>();
        Observable.Generate(1, _ => true, x => x + 1, x => x.ToString(CultureInfo.InvariantCulture)).Take(5).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { "1", "2", "3", "4", "5" }));
    }

    [Test]
    public void Generate_ShouldStopProducingWhenUnsubscribed()
    {
        var count = 0;
        Subscriber<int>? subscriber = null;
        subscriber = Subscriber.Create<int>(onNext: x =>
        {
            count++;
            if (x == 2)
            {
                subscriber!.Dispose();
            }
        });

        Observable.Generate(1, x => x < 4, x => x + 1).Subscribe(subscriber);

        Assert.That(count, Is.EqualTo(2));
    }

    [Test]
    public void Generate_ShouldEmitErrorIfResultSelectorThrows()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;
        var results = new List<int>();
        Observable.Generate<int, int>(1, x => x < 3, x => x * 2, x => throw error).Subscribe(results.Add, err => received = err);

        Assert.That(results, Is.Empty);
        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void Generate_ShouldEmitErrorAfterFirstValueIfIterateFunctionThrows()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;
        var results = new List<int>();
        Observable.Generate<int>(1, _ => true, x => throw error).Subscribe(results.Add, err => received = err);

        Assert.That(results, Is.EqualTo(new[] { 1 }));
        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void Generate_ShouldEmitErrorIfConditionFunctionThrows()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;
        var results = new List<int>();
        Observable.Generate<int>(1, x => throw error, x => x + 1).Subscribe(results.Add, err => received = err);

        Assert.That(results, Is.Empty);
        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void Generate_WithAScheduler_ShouldEventuallyProduceAllValues()
    {
        using var signal = new ManualResetEventSlim();
        var results = new List<int>();

        Observable.Generate(1, x => x < 4, x => x + 1, x => x, TaskPoolScheduler.Instance).Subscribe(
            results.Add,
            onComplete: () => signal.Set());

        Assert.That(signal.Wait(TimeSpan.FromSeconds(2)), Is.True);
        Assert.That(results, Is.EqualTo(new[] { 1, 2, 3 }));
    }

    // Ported (non-marble subset) from rxjs 7.8.2 spec/observables/if-spec.ts ("iif").
    [Test]
    public void Iif_ShouldSubscribeToThenSourceWhenConditionalReturnsTrue()
    {
        var results = new List<string>();
        Observable.Iif(() => true, Observable.Of("a"), Observable.Of<string>()).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { "a" }));
    }

    [Test]
    public void Iif_ShouldSubscribeToElseSourceWhenConditionalReturnsFalse()
    {
        var results = new List<string>();
        Observable.Iif(() => false, Observable.Of("a"), Observable.Of("b")).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { "b" }));
    }

    [Test]
    public void Iif_ShouldCompleteWithoutAnElseSourceWhenConditionalReturnsFalse()
    {
        var results = new List<string>();
        var completed = false;
        Observable.Iif(() => false, Observable.Of("a"), Observable.Of<string>()).Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.Empty);
        Assert.That(completed, Is.True);
    }

    [Test]
    public void Iif_ShouldRaiseErrorWhenConditionalThrows()
    {
        Exception? received = null;
        Observable.Iif<string>(
                () => throw new InvalidOperationException("boom"),
                Observable.Of("a"),
                Observable.Of<string>())
            .Subscribe(onError: err => received = err);

        Assert.That(received, Is.TypeOf<InvalidOperationException>());
    }

    [Test]
    public void Iif_ShouldEvaluateTheConditionLazilyPerSubscription()
    {
        var flag = true;
        var source = Observable.Iif(() => flag, Observable.Of("a"), Observable.Of("b"));

        var firstResults = new List<string>();
        source.Subscribe(firstResults.Add);

        flag = false;
        var secondResults = new List<string>();
        source.Subscribe(secondResults.Add);

        Assert.That(firstResults, Is.EqualTo(new[] { "a" }));
        Assert.That(secondResults, Is.EqualTo(new[] { "b" }));
    }

    // Ported (non-marble subset) from rxjs 7.8.2 spec/observables/using-spec.ts.
    [Test]
    public void Using_ShouldDisposeOfTheResourceWhenTheSubscriptionIsDisposed()
    {
        var disposed = false;

        Observable.Using(() => new Subscription(() => disposed = true), (Subscription _) => Observable.Range(0, 3))
            .Take(2)
            .Subscribe();

        Assert.That(disposed, Is.True);
    }

    [Test]
    public void Using_ShouldRaiseErrorWhenTheResourceFactoryThrows()
    {
        var expectedError = new InvalidOperationException("resource factory boom");
        var observableFactoryCalled = false;
        Exception? received = null;

        Observable.Using<int, Subscription>(
                () => throw expectedError,
                _ =>
                {
                    observableFactoryCalled = true;
                    return Observable.Of(1);
                })
            .Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(expectedError));
        Assert.That(observableFactoryCalled, Is.False);
    }

    [Test]
    public void Using_ShouldRaiseErrorAndStillDisposeTheResourceWhenTheObservableFactoryThrows()
    {
        var error = new InvalidOperationException("observable factory boom");
        var disposed = false;
        Exception? received = null;

        Observable.Using<int, Subscription>(
                () => new Subscription(() => disposed = true),
                _ => throw error)
            .Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
        Assert.That(disposed, Is.True);
    }

    [Test]
    public void Using_ShouldCreateAFreshResourceAndSourceForEachSubscription()
    {
        var resourceFactoryCalls = 0;
        var observableFactoryCalls = 0;

        var source = Observable.Using(
            () =>
            {
                resourceFactoryCalls++;
                return new Subscription(Observable.Noop);
            },
            (Subscription _) =>
            {
                observableFactoryCalls++;
                return Observable.Of(1);
            });

        source.Subscribe();
        source.Subscribe();

        Assert.That(resourceFactoryCalls, Is.EqualTo(2));
        Assert.That(observableFactoryCalls, Is.EqualTo(2));
    }

    [Test]
    public void Using_ShouldDisposeTheResourceOnExternalUnsubscribeBeforeTheSourceCompletes()
    {
        var disposed = false;
        var subscription = Observable.Using(() => new Subscription(() => disposed = true), (Subscription _) => Observable.Never<int>())
            .Subscribe();

        Assert.That(disposed, Is.False);
        subscription.Dispose();

        Assert.That(disposed, Is.True);
    }

    [Test]
    public void Using_ShouldDisposeTheResourceAfterTheSourcesOwnTeardownRuns()
    {
        var order = new List<string>();

        var subscription = Observable.Using(
                () => new Subscription(() => order.Add("resource")),
                (Subscription _) => new Observable<int>(subscriber => subscriber.Add(() => order.Add("source"))))
            .Subscribe();

        subscription.Dispose();

        Assert.That(order, Is.EqualTo(new[] { "source", "resource" }));
    }

    [Test]
    public void Identity_ShouldReturnItsArgumentUnchanged()
        => Assert.That(Observable.Identity(42), Is.EqualTo(42));

    [Test]
    public void Noop_ShouldNotThrow()
        => Assert.DoesNotThrow(() => Observable.Noop());

    private sealed class TestEventPublisher
    {
        public event EventHandler<int>? Changed;

        public bool HasSubscribers => Changed is not null;

        public void RaiseChanged(int value) => Changed?.Invoke(this, value);
    }
}
