using RxSharp.Operators;

namespace RxSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/mergeScan-spec.ts.
[TestFixture]
public class MergeScanTests
{
    [Test]
    public void ShouldAccumulateValuesLikeARunningCount()
    {
        var results = new List<int>();
        Observable.Of(1, 1, 1, 1).MergeScan((acc, one) => Observable.Of(acc + one), 0).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 1, 2, 3, 4 }));
    }

    [Test]
    public void ShouldPropagateErrorsFromTheAccumulator()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;
        Observable.Of(1).MergeScan<int, int>((_, _, _) => throw error, 0).Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldPropagateErrorsFromAnInnerObservable()
    {
        var error = new InvalidOperationException("inner boom");
        Exception? received = null;
        Observable.Of(1).MergeScan((_, _) => Observable.ThrowError<int>(() => error), 0).Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }
}
