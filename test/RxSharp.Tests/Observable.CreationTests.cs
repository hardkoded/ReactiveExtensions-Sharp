using RxSharp.Operators;

namespace RxSharp.Tests;

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

    [Test]
    public void Merge_ShouldEmitFromAllSourcesConcurrentlyAndCompleteWhenAllHaveCompleted()
    {
        var subjectA = new RxSharp.Subjects.Subject<int>();
        var subjectB = new RxSharp.Subjects.Subject<int>();
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

    private sealed class TestEventPublisher
    {
        public event EventHandler<int>? Changed;

        public bool HasSubscribers => Changed is not null;

        public void RaiseChanged(int value) => Changed?.Invoke(this, value);
    }
}
