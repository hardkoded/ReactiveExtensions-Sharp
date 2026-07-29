namespace ReactiveExtensionsSharp.Tests;

// Ported from rxjs 7.8.2 spec/Subscriber-spec.ts (SafeSubscriber describe block).
[TestFixture]
public class SubscriberTests
{
    [Test]
    public void ShouldIgnoreNextMessagesAfterUnsubscription()
    {
        var times = 0;
        var sub = Subscriber.Create<Unit>(_ => times++);

        sub.OnNext(Unit.Default);
        sub.OnNext(Unit.Default);
        sub.Unsubscribe();
        sub.OnNext(Unit.Default);

        Assert.That(times, Is.EqualTo(2));
    }

    [Test]
    public void ShouldIgnoreErrorMessagesAfterUnsubscription()
    {
        var times = 0;
        var errorCalled = false;
        var sub = Subscriber.Create<Unit>(_ => times++, _ => errorCalled = true);

        sub.OnNext(Unit.Default);
        sub.OnNext(Unit.Default);
        sub.Unsubscribe();
        sub.OnNext(Unit.Default);
        sub.OnError(new InvalidOperationException());

        Assert.That(times, Is.EqualTo(2));
        Assert.That(errorCalled, Is.False);
    }

    [Test]
    public void ShouldIgnoreCompleteMessagesAfterUnsubscription()
    {
        var times = 0;
        var completeCalled = false;
        var sub = Subscriber.Create<Unit>(_ => times++, onComplete: () => completeCalled = true);

        sub.OnNext(Unit.Default);
        sub.OnNext(Unit.Default);
        sub.Unsubscribe();
        sub.OnNext(Unit.Default);
        sub.OnCompleted();

        Assert.That(times, Is.EqualTo(2));
        Assert.That(completeCalled, Is.False);
    }

    [Test]
    public void ShouldHaveIdempotentUnsubscription()
    {
        var count = 0;
        var subscriber = Subscriber.Create<Unit>();
        subscriber.Add(() => count++);

        Assert.That(count, Is.EqualTo(0));

        subscriber.Unsubscribe();
        Assert.That(count, Is.EqualTo(1));

        subscriber.Unsubscribe();
        Assert.That(count, Is.EqualTo(1));
    }

    [Test]
    public void ShouldUnsubscribeAndUnregisterAllFinalizersAfterComplete()
    {
        var isUnsubscribed = false;
        var subscriber = Subscriber.Create<Unit>();
        subscriber.Add(() => isUnsubscribed = true);

        subscriber.OnCompleted();

        Assert.That(isUnsubscribed, Is.True);
        Assert.That(subscriber.IsDisposed, Is.True);
    }

    [Test]
    public void ShouldUnsubscribeAndUnregisterAllFinalizersAfterError()
    {
        var isTornDown = false;
        var subscriber = Subscriber.Create<Unit>(onError: _ => { });
        subscriber.Add(() => isTornDown = true);

        subscriber.OnError(new InvalidOperationException("test"));

        Assert.That(isTornDown, Is.True);
        Assert.That(subscriber.IsDisposed, Is.True);
    }
}
