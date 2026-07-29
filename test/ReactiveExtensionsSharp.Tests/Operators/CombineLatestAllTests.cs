using ReactiveExtensionsSharp.Operators;

namespace ReactiveExtensionsSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/combineLatestAll-spec.ts.
[TestFixture]
public class CombineLatestAllTests
{
    [Test]
    public void ShouldCombineEveryCollectedInnerObservableOnceOuterCompletes()
    {
        var results = new List<IReadOnlyList<int>>();
        var completed = false;

        Observable.Of(Observable.Of(1, 2), Observable.Of(10, 20))
            .CombineLatestAll()
            .Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results.Select(r => r.ToArray()), Is.EqualTo(new[] { new[] { 2, 10 }, new[] { 2, 20 } }));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldEmitNothingIfOuterCompletesWithoutAnyInner()
    {
        var results = new List<IReadOnlyList<int>>();
        var completed = false;

        Observable.Empty<Observable<int>>().CombineLatestAll().Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.Empty);
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldForwardAnErrorFromAnyCollectedInner()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;

        Observable.Of(Observable.Of(1), Observable.ThrowError<int>(() => error))
            .CombineLatestAll()
            .Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }
}
