namespace RxSharp.Tests;

// Ported from rxjs 7.8.2 spec/Observable-spec.ts (non-marble cases).
[TestFixture]
public class ObservableTests
{
    [Test]
    public void ShouldBeConstructedWithASubscriberFunction()
    {
        var source = new Observable<int>(observer =>
        {
            observer.OnNext(1);
            observer.OnCompleted();
        });

        var received = -1;
        var completed = false;
        source.Subscribe(x => received = x, onComplete: () => completed = true);

        Assert.That(received, Is.EqualTo(1));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldSendErrorsThrownInTheConstructorDownTheErrorPath()
    {
        Exception? received = null;
        new Observable<int>(_ => throw new InvalidOperationException("this should be handled"))
            .Subscribe(onError: err => received = err);

        Assert.That(received, Is.Not.Null);
        Assert.That(received!.Message, Is.EqualTo("this should be handled"));
    }

    [Test]
    public void ShouldAllowSubscribingWithOnlyANextCallback()
    {
        var values = new List<int>();
        Observable.Of(1, 2, 3).Subscribe(values.Add);

        Assert.That(values, Is.EqualTo(new[] { 1, 2, 3 }));
    }

    [Test]
    public void ShouldCallTeardownLogicOnUnsubscribe()
    {
        var tornDown = false;
        var source = new Observable<int>(_ => new Subscription(() => tornDown = true));

        var subscription = source.Subscribe();
        subscription.Dispose();

        Assert.That(tornDown, Is.True);
    }

    [Test]
    public void ShouldNotCallTeardownTwiceForAnAlreadyDisposedSubscriber()
    {
        var teardownCalls = 0;
        var source = new Observable<int>(subscriber =>
        {
            subscriber.OnCompleted();
            return new Subscription(() => teardownCalls++);
        });

        source.Subscribe();

        Assert.That(teardownCalls, Is.EqualTo(1));
    }
}
