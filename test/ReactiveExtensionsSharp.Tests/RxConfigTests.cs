namespace ReactiveExtensionsSharp.Tests;

[TestFixture]
public class RxConfigTests
{
    [TearDown]
    public void TearDown() => RxConfig.ResetOnUnhandledError();

    [Test]
    public void ShouldReportUnhandledErrorsWithoutThrowingOnTheSubscribingThread()
    {
        Exception? reported = null;
        using var signal = new ManualResetEventSlim();
        RxConfig.OnUnhandledError = err =>
        {
            reported = err;
            signal.Set();
        };

        var error = new InvalidOperationException("nobody is listening");

        // No onError handler at all: must not throw synchronously out of Subscribe.
        Assert.DoesNotThrow(() => Observable.ThrowError<int>(() => error).Subscribe());

        Assert.That(signal.Wait(TimeSpan.FromSeconds(1)), Is.True);
        Assert.That(reported, Is.SameAs(error));
    }
}
