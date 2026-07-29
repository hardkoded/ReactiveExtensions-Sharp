using ReactiveExtensionsSharp.Operators;

namespace ReactiveExtensionsSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/zipWith-spec.ts.
[TestFixture]
public class ZipWithTests
{
    [Test]
    public void ShouldCombinePositionallyAndCompleteWhenTheShortestSourceIsExhausted()
    {
        var results = new List<IReadOnlyList<int>>();
        var completed = false;

        Observable.Of(1, 2, 3).ZipWith(Observable.Of(10, 20)).Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results.Select(r => r.ToArray()), Is.EqualTo(new[] { new[] { 1, 10 }, new[] { 2, 20 } }));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldForwardAnErrorFromEitherSide()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;

        Observable.Of(1).ZipWith(Observable.ThrowError<int>(() => error)).Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }
}
