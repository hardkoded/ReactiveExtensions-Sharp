using RxSharp.Extras;

namespace RxSharp.Tests.Extras;

[TestFixture]
public class AssumeNeverEmitsExtrasTests
{
    [Test]
    public void ShouldPropagateTheErrorFromAnErrorOnlySource()
    {
        var error = new InvalidOperationException("boom");
        using var signal = new ManualResetEventSlim();
        Exception? received = null;

        Observable.ThrowError<Unit>(() => error)
            .AssumeNeverEmits<string>()
            .Subscribe(onError: err =>
            {
                received = err;
                signal.Set();
            });

        Assert.That(signal.Wait(TimeSpan.FromSeconds(2)), Is.True);
        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldCompleteWithoutEmittingIfTheSourceCompletesWithoutErroring()
    {
        using var signal = new ManualResetEventSlim();
        var completed = false;
        var received = new List<string>();

        Observable.Empty<Unit>()
            .AssumeNeverEmits<string>()
            .Subscribe(received.Add, onComplete: () =>
            {
                completed = true;
                signal.Set();
            });

        Assert.That(signal.Wait(TimeSpan.FromSeconds(2)), Is.True);
        Assert.That(completed, Is.True);
        Assert.That(received, Is.Empty);
    }

    [Test]
    public void ShouldThrowIfTheSourceViolatesTheNeverEmitsContractByEmittingAValue()
    {
        using var signal = new ManualResetEventSlim();
        Exception? received = null;

        Observable.Of(Unit.Default)
            .AssumeNeverEmits<string>()
            .Subscribe(onError: err =>
            {
                received = err;
                signal.Set();
            });

        Assert.That(signal.Wait(TimeSpan.FromSeconds(2)), Is.True);
        Assert.That(received, Is.InstanceOf<InvalidOperationException>());
    }
}
