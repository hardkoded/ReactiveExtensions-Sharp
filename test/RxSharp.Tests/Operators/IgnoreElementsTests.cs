using RxSharp.Operators;

namespace RxSharp.Tests.Operators;

// Ported from rxjs 7.8.2 spec/operators/ignoreElements-spec.ts (non-marble cases).
[TestFixture]
public class IgnoreElementsTests
{
    [Test]
    public void ShouldIgnoreAllNextEmissionsButPassComplete()
    {
        var nextCalls = 0;
        var completed = false;
        Observable.Of("you", "talking", "to", "me").IgnoreElements().Subscribe(_ => nextCalls++, onComplete: () => completed = true);

        Assert.That(nextCalls, Is.EqualTo(0));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldIgnoreAllNextEmissionsButPassError()
    {
        var error = new InvalidOperationException("boom");
        var nextCalls = 0;
        Exception? received = null;
        Observable.ThrowError<string>(() => error).IgnoreElements().Subscribe(_ => nextCalls++, onError: err => received = err);

        Assert.That(nextCalls, Is.EqualTo(0));
        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldHandleAnEmptySource()
    {
        var nextCalls = 0;
        var completed = false;
        Observable.Empty<int>().IgnoreElements().Subscribe(_ => nextCalls++, onComplete: () => completed = true);

        Assert.That(nextCalls, Is.EqualTo(0));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldHandleANeverSource()
    {
        var nextCalls = 0;
        var completed = false;
        Observable.Never<int>().IgnoreElements().Subscribe(_ => nextCalls++, onComplete: () => completed = true);

        Assert.That(nextCalls, Is.EqualTo(0));
        Assert.That(completed, Is.False);
    }
}
