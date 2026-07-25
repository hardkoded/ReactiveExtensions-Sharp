using RxSharp.Operators;

namespace RxSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/delay-spec.ts.
[TestFixture]
public class DelayTests
{
    [Test]
    public void ShouldDelayEachValueByTheGivenTimeSpan()
    {
        using var signal = new ManualResetEventSlim();
        var results = new List<int>();

        Observable.Of(1, 2, 3).Delay(TimeSpan.FromMilliseconds(20)).Subscribe(results.Add, onComplete: () => signal.Set());

        Assert.That(results, Is.Empty, "should not emit synchronously");
        Assert.That(signal.Wait(TimeSpan.FromSeconds(2)), Is.True);
        Assert.That(results, Is.EqualTo(new[] { 1, 2, 3 }));
    }

    [Test]
    public void ShouldPropagateErrorsFromTheSource()
    {
        using var signal = new ManualResetEventSlim();
        var error = new InvalidOperationException("boom");
        Exception? received = null;

        Observable.ThrowError<int>(() => error).Delay(TimeSpan.FromMilliseconds(10)).Subscribe(onError: err =>
        {
            received = err;
            signal.Set();
        });

        Assert.That(signal.Wait(TimeSpan.FromSeconds(2)), Is.True);
        Assert.That(received, Is.SameAs(error));
    }
}
