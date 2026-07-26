using RxSharp.Operators;

namespace RxSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/mapTo-spec.ts.
[TestFixture]
public class MapToTests
{
    [Test]
    public void ShouldEmitTheConstantValueForEverySourceValue()
    {
        var results = new List<string>();
        Observable.Of(1, 2, 3).MapTo("x").Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { "x", "x", "x" }));
    }

    [Test]
    public void ShouldForwardErrorsUnaffected()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;

        Observable.ThrowError<int>(() => error).MapTo("x").Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldCompleteWithoutEmittingOnAnEmptySource()
    {
        var results = new List<string>();
        var completed = false;
        Observable.Empty<int>().MapTo("x").Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.Empty);
        Assert.That(completed, Is.True);
    }
}
