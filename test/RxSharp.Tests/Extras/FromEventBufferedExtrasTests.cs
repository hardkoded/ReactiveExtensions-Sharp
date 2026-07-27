using RxSharp.Extras;

namespace RxSharp.Tests.Extras;

[TestFixture]
public class FromEventBufferedExtrasTests
{
    private sealed class FakeTargetManager
    {
        public event EventHandler<string>? TargetCreated;

        public void RaiseTargetCreated(string targetId) => TargetCreated?.Invoke(this, targetId);
    }

    [Test]
    public void ShouldAttachTheHandlerImmediatelyRatherThanAtSubscribeTime()
    {
        var manager = new FakeTargetManager();

        using var source = PuppeteerExtras.FromEventBuffered<string>(
            h => manager.TargetCreated += h,
            h => manager.TargetCreated -= h);

        // Raised before AsObservable() is ever subscribed to - this is the exact gap FromEventBuffered exists
        // to close (a plain, cold FromEvent would lose this).
        manager.RaiseTargetCreated("early-target");

        var received = new List<string>();
        source.AsObservable().Subscribe(received.Add);

        Assert.That(received, Is.EqualTo(new[] { "early-target" }));
    }

    [Test]
    public void ShouldReplayBufferedValuesToALateSubscriberThenDeliverLiveValues()
    {
        var manager = new FakeTargetManager();
        using var source = PuppeteerExtras.FromEventBuffered<string>(
            h => manager.TargetCreated += h,
            h => manager.TargetCreated -= h);

        manager.RaiseTargetCreated("buffered-target");

        var received = new List<string>();
        source.AsObservable().Subscribe(received.Add);
        manager.RaiseTargetCreated("live-target");

        Assert.That(received, Is.EqualTo(new[] { "buffered-target", "live-target" }));
    }

    [Test]
    public void ShouldDeliverToEveryIndependentSubscriber()
    {
        var manager = new FakeTargetManager();
        using var source = PuppeteerExtras.FromEventBuffered<string>(
            h => manager.TargetCreated += h,
            h => manager.TargetCreated -= h);

        var first = new List<string>();
        var second = new List<string>();
        source.AsObservable().Subscribe(first.Add);
        source.AsObservable().Subscribe(second.Add);

        manager.RaiseTargetCreated("shared-target");

        Assert.That(first, Is.EqualTo(new[] { "shared-target" }));
        Assert.That(second, Is.EqualTo(new[] { "shared-target" }));
    }

    [Test]
    public void ShouldOnlyReplayUpToTheRequestedBufferSize()
    {
        var manager = new FakeTargetManager();
        using var source = PuppeteerExtras.FromEventBuffered<string>(
            h => manager.TargetCreated += h,
            h => manager.TargetCreated -= h,
            bufferSize: 2);

        manager.RaiseTargetCreated("one");
        manager.RaiseTargetCreated("two");
        manager.RaiseTargetCreated("three");

        var received = new List<string>();
        source.AsObservable().Subscribe(received.Add);

        Assert.That(received, Is.EqualTo(new[] { "two", "three" }));
    }

    [Test]
    public void ShouldDetachTheHandlerOnDispose()
    {
        var manager = new FakeTargetManager();
        var source = PuppeteerExtras.FromEventBuffered<string>(
            h => manager.TargetCreated += h,
            h => manager.TargetCreated -= h);

        source.Dispose();
        manager.RaiseTargetCreated("after-dispose");

        // If the handler weren't detached, this would throw ObjectDisposedException from the underlying
        // subject instead of silently doing nothing.
        Assert.DoesNotThrow(() => manager.RaiseTargetCreated("another-after-dispose"));
    }

    [Test]
    public void ShouldBeSafeToDisposeMoreThanOnce()
    {
        var manager = new FakeTargetManager();
        var source = PuppeteerExtras.FromEventBuffered<string>(
            h => manager.TargetCreated += h,
            h => manager.TargetCreated -= h);

        source.Dispose();
        Assert.DoesNotThrow(() => source.Dispose());
    }
}
