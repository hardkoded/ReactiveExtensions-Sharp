using ReactiveExtensionsSharp.Operators;
using ReactiveExtensionsSharp.Subjects;

namespace ReactiveExtensionsSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/mergeAll-spec.ts.
[TestFixture]
public class MergeAllTests
{
    [Test]
    public void ShouldMergeAHigherOrderObservableOfObservables()
    {
        var a = new Subject<int>();
        var b = new Subject<int>();
        var outer = new Subject<Observable<int>>();
        var results = new List<int>();

        outer.AsObservable().MergeAll().Subscribe(results.Add);

        outer.OnNext(a.AsObservable());
        outer.OnNext(b.AsObservable());
        a.OnNext(1);
        b.OnNext(10);
        a.OnNext(2);

        Assert.That(results, Is.EqualTo(new[] { 1, 10, 2 }));
    }

    [Test]
    public void ShouldCompleteOnceOuterAndEveryInnerHaveCompleted()
    {
        var completed = false;
        Observable.Of(Observable.Of(1), Observable.Of(2)).MergeAll().Subscribe(onComplete: () => completed = true);

        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldForwardAnErrorFromAnInnerObservable()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;

        Observable.Of(Observable.ThrowError<int>(() => error)).MergeAll().Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }
}
