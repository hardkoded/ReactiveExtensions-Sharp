using ReactiveExtensionsSharp.Operators;

namespace ReactiveExtensionsSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/concatWith-spec.ts.
[TestFixture]
public class ConcatWithTests
{
    [Test]
    public void ShouldConcatenateSourceWithTheOtherSourcesInOrder()
    {
        var results = new List<int>();
        Observable.Of(1, 2).ConcatWith(Observable.Of(3, 4), Observable.Of(5)).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 1, 2, 3, 4, 5 }));
    }

    [Test]
    public void ShouldReturnSourceUnchangedWhenGivenNoOtherSources()
    {
        var results = new List<int>();
        Observable.Of(1, 2, 3).ConcatWith().Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 1, 2, 3 }));
    }

    [Test]
    public void ShouldForwardAnErrorAndNeverSubscribeToTheOthers()
    {
        var error = new InvalidOperationException("boom");
        var otherSubscribed = false;
        var other = new Observable<int>(_ => otherSubscribed = true);
        Exception? received = null;

        Observable.ThrowError<int>(() => error).ConcatWith(other).Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
        Assert.That(otherSubscribed, Is.False);
    }
}
