using ReactiveExtensionsSharp.Operators;

namespace ReactiveExtensionsSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/mergeMapTo-spec.ts.
[TestFixture]
public class MergeMapToTests
{
    [Test]
    public void ShouldSubscribeToTheSameInnerForEverySourceValue()
    {
        var results = new List<int>();
        var completed = false;

        Observable.Of("a", "b").MergeMapTo(Observable.Of(1, 2)).Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.EqualTo(new[] { 1, 2, 1, 2 }));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldForwardAnErrorFromTheInnerObservable()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;

        Observable.Of(1).MergeMapTo(Observable.ThrowError<int>(() => error)).Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldCompleteWithoutSubscribingToTheInnerOnAnEmptySource()
    {
        var innerSubscribed = false;
        var completed = false;
        var inner = new Observable<int>(_ => innerSubscribed = true);

        Observable.Empty<int>().MergeMapTo(inner).Subscribe(onComplete: () => completed = true);

        Assert.That(innerSubscribed, Is.False);
        Assert.That(completed, Is.True);
    }
}
