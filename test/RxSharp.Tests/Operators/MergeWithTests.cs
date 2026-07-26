using RxSharp.Operators;
using RxSharp.Subjects;

namespace RxSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/mergeWith-spec.ts.
[TestFixture]
public class MergeWithTests
{
    [Test]
    public void ShouldMergeSourceWithTheOtherSourcesAsTheyArrive()
    {
        var a = new Subject<int>();
        var b = new Subject<int>();
        var results = new List<int>();

        a.AsObservable().MergeWith(b.AsObservable()).Subscribe(results.Add);

        a.OnNext(1);
        b.OnNext(10);
        a.OnNext(2);

        Assert.That(results, Is.EqualTo(new[] { 1, 10, 2 }));
    }

    [Test]
    public void ShouldReturnSourceUnchangedWhenGivenNoOtherSources()
    {
        var results = new List<int>();
        Observable.Of(1, 2, 3).MergeWith().Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 1, 2, 3 }));
    }

    [Test]
    public void ShouldCompleteOnceSourceAndEveryOtherHaveCompleted()
    {
        var a = new Subject<int>();
        var b = new Subject<int>();
        var completed = false;

        a.AsObservable().MergeWith(b.AsObservable()).Subscribe(onComplete: () => completed = true);

        a.OnCompleted();
        Assert.That(completed, Is.False);

        b.OnCompleted();
        Assert.That(completed, Is.True);
    }
}
