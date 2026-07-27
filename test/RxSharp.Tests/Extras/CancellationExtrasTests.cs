using RxSharp.Extras;

namespace RxSharp.Tests.Extras;

[TestFixture]
public class CancellationExtrasTests
{
    [Test]
    public void ShouldErrorImmediatelyIfTokenIsAlreadyCancelled()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Exception? received = null;
        RxExtensions.FromCancellationToken(cts.Token).Subscribe(onError: err => received = err);

        Assert.That(received, Is.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public void ShouldErrorAsSoonAsTheTokenIsCancelled()
    {
        using var cts = new CancellationTokenSource();
        Exception? received = null;
        RxExtensions.FromCancellationToken(cts.Token).Subscribe(onError: err => received = err);

        Assert.That(received, Is.Null);

        cts.Cancel();

        Assert.That(received, Is.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public void ShouldNeverEmitAValue()
    {
        using var cts = new CancellationTokenSource();
        var nextCalled = false;
        var subscription = RxExtensions.FromCancellationToken(cts.Token).Subscribe(_ => nextCalled = true);

        subscription.Dispose();
        cts.Cancel();

        Assert.That(nextCalled, Is.False);
    }

    [Test]
    public void ShouldUseTheCustomCauseFactory()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        var cause = new InvalidOperationException("custom cause");

        Exception? received = null;
        RxExtensions.FromCancellationToken(cts.Token, () => cause).Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(cause));
    }
}
