using ReactiveExtensionsSharp.Extras;

namespace ReactiveExtensionsSharp.Tests.Extras;

[TestFixture]
public class TimeoutExtrasTests
{
    [Test]
    public void ShouldErrorAfterTheDelay()
    {
        using var signal = new ManualResetEventSlim();
        Exception? received = null;

        RxExtensions.Timeout(TimeSpan.FromMilliseconds(20)).Subscribe(onError: err =>
        {
            received = err;
            signal.Set();
        });

        Assert.That(signal.Wait(TimeSpan.FromSeconds(2)), Is.True);
        Assert.That(received, Is.InstanceOf<TimeoutException>());
    }

    [Test]
    public void ShouldNeverErrorWhenDelayIsZero()
    {
        var errored = false;
        RxExtensions.Timeout(TimeSpan.Zero).Subscribe(onError: _ => errored = true);

        Thread.Sleep(50);

        Assert.That(errored, Is.False);
    }

    [Test]
    public void ShouldUseTheCustomCauseFactory()
    {
        using var signal = new ManualResetEventSlim();
        var cause = new InvalidOperationException("custom timeout cause");
        Exception? received = null;

        RxExtensions.Timeout(TimeSpan.FromMilliseconds(10), () => cause).Subscribe(onError: err =>
        {
            received = err;
            signal.Set();
        });

        Assert.That(signal.Wait(TimeSpan.FromSeconds(2)), Is.True);
        Assert.That(received, Is.SameAs(cause));
    }
}
