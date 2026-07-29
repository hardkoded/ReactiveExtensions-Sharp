using ReactiveExtensionsSharp.Operators;

namespace ReactiveExtensionsSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/zipAll-spec.ts.
[TestFixture]
public class ZipAllTests
{
    [Test]
    public void ShouldZipEveryCollectedInnerObservableOnceOuterCompletes()
    {
        var results = new List<IReadOnlyList<int>>();
        var completed = false;

        Observable.Of(Observable.Of(1, 2, 3), Observable.Of(10, 20))
            .ZipAll()
            .Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results.Select(r => r.ToArray()), Is.EqualTo(new[] { new[] { 1, 10 }, new[] { 2, 20 } }));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldEmitNothingIfOuterCompletesWithoutAnyInner()
    {
        var results = new List<IReadOnlyList<int>>();
        var completed = false;

        Observable.Empty<Observable<int>>().ZipAll().Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.Empty);
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldForwardAnErrorFromAnyCollectedInner()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;

        Observable.Of(Observable.Of(1), Observable.ThrowError<int>(() => error))
            .ZipAll()
            .Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }
}
