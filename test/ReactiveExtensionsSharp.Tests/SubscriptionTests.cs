namespace ReactiveExtensionsSharp.Tests;

// Ported from rxjs 7.8.2 spec/Subscription-spec.ts.
[TestFixture]
public class SubscriptionTests
{
    [Test]
    public void Add_ShouldUnsubscribeChildSubscriptions()
    {
        var main = new Subscription();
        var isCalled = false;
        var child = new Subscription(() => isCalled = true);

        main.Add(child);
        main.Unsubscribe();

        Assert.That(isCalled, Is.True);
    }

    [Test]
    public void Add_ShouldUnsubscribeChildImmediately_WhenAlreadyUnsubscribed()
    {
        var main = new Subscription();
        main.Unsubscribe();

        var isCalled = false;
        var child = new Subscription(() => isCalled = true);
        main.Add(child);

        Assert.That(isCalled, Is.True);
    }

    [Test]
    public void Add_ShouldUnsubscribeAFinalizerFunctionThatWasPassed()
    {
        var isCalled = false;
        var main = new Subscription();
        main.Add(() => isCalled = true);
        main.Unsubscribe();

        Assert.That(isCalled, Is.True);
    }

    [Test]
    public void Add_ShouldUnsubscribeAFinalizerFunctionImmediately_WhenAlreadyUnsubscribed()
    {
        var isCalled = false;
        var main = new Subscription();
        main.Unsubscribe();
        main.Add(() => isCalled = true);

        Assert.That(isCalled, Is.True);
    }

    [Test]
    public void Add_ShouldUnsubscribeAnUnsubscribableWhenUnsubscribed()
    {
        var isCalled = false;
        var main = new Subscription();
        main.Add(new Subscription(() => isCalled = true));
        main.Unsubscribe();

        Assert.That(isCalled, Is.True);
    }

    [Test]
    public void Remove_ShouldRemoveAddedSubscriptions()
    {
        var isCalled = false;
        var main = new Subscription();
        var child = new Subscription(() => isCalled = true);

        main.Add(child);
        main.Remove(child);
        main.Unsubscribe();

        Assert.That(isCalled, Is.False);
    }

    [Test]
    public void Unsubscribe_ShouldBeIdempotent()
    {
        var count = 0;
        var subscription = new Subscription(() => count++);

        Assert.That(count, Is.EqualTo(0));

        subscription.Unsubscribe();
        Assert.That(count, Is.EqualTo(1));

        subscription.Unsubscribe();
        Assert.That(count, Is.EqualTo(1));
    }

    [Test]
    public void Unsubscribe_ShouldAggregateExceptionsFromMultipleFinalizers()
    {
        var subscription = new Subscription();
        subscription.Add(() => throw new InvalidOperationException("first"));
        subscription.Add(() => throw new InvalidOperationException("second"));

        var ex = Assert.Throws<UnsubscriptionException>(() => subscription.Unsubscribe());
        Assert.That(ex!.Errors, Has.Count.EqualTo(2));
    }
}
