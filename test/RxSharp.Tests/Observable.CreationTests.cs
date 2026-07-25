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
}
