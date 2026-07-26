using RxSharp.Operators;
using RxSharp.Subjects;

namespace RxSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/combineLatestWith-spec.ts.
[TestFixture]
public class CombineLatestWithTests
{
    [Test]
    public void ShouldCombineTheLatestValueFromSourceAndTheOthers()
    {
        var a = new Subject<int>();
        var b = new Subject<int>();
        var results = new List<IReadOnlyList<int>>();

        a.AsObservable().CombineLatestWith(b.AsObservable()).Subscribe(results.Add);

        a.OnNext(1);
        b.OnNext(10);
        a.OnNext(2);

        Assert.That(results.Select(r => r.ToArray()), Is.EqualTo(new[] { new[] { 1, 10 }, new[] { 2, 10 } }));
    }

    [Test]
    public void ShouldForwardAnErrorFromEitherSide()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;

        Observable.Of(1).CombineLatestWith(Observable.ThrowError<int>(() => error)).Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }
}
