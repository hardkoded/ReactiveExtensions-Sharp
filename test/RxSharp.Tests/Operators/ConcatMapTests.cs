using RxSharp.Operators;
using RxSharp.Subjects;

namespace RxSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/concatMap-spec.ts.
[TestFixture]
public class ConcatMapTests
{
    [Test]
    public void ShouldMapAndFlattenInEmissionOrder()
    {
        var results = new List<int>();
        Observable.Of(1, 2, 3).ConcatMap(x => Observable.Of(x, x * 10)).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 1, 10, 2, 20, 3, 30 }));
    }

    [Test]
    public void ShouldWaitForEachInnerToCompleteBeforeStartingTheNext()
    {
        var subjectA = new Subject<string>();
        var subjectB = new Subject<string>();
        var results = new List<string>();

        Observable.Of(subjectA.AsObservable(), subjectB.AsObservable()).ConcatMap(inner => inner).Subscribe(results.Add);

        subjectB.OnNext("from-b-too-early");
        Assert.That(results, Is.Empty, "B hasn't started yet — A hasn't completed.");

        subjectA.OnNext("a1");
        subjectA.OnCompleted();

        subjectB.OnNext("b1");
        subjectB.OnCompleted();

        Assert.That(results, Is.EqualTo(new[] { "a1", "b1" }));
    }

    [Test]
    public void ShouldPropagateErrorsFromAnInnerObservable()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;
        Observable.Of(1, 2).ConcatMap(_ => Observable.ThrowError<int>(() => error)).Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }
}
