using ReactiveExtensionsSharp.Operators;
using ReactiveExtensionsSharp.Subjects;

namespace ReactiveExtensionsSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/switchAll-spec.ts.
[TestFixture]
public class SwitchAllTests
{
    [Test]
    public void ShouldMirrorOnlyTheMostRecentlyProducedInnerObservable()
    {
        var a = new Subject<int>();
        var b = new Subject<int>();
        var outer = new Subject<Observable<int>>();
        var results = new List<int>();

        outer.AsObservable().SwitchAll().Subscribe(results.Add);

        outer.OnNext(a.AsObservable());
        a.OnNext(1);
        outer.OnNext(b.AsObservable());
        a.OnNext(99);
        b.OnNext(2);

        Assert.That(results, Is.EqualTo(new[] { 1, 2 }));
    }

    [Test]
    public void ShouldCompleteOnceOuterAndTheCurrentInnerHaveCompleted()
    {
        var completed = false;
        Observable.Of(Observable.Of(1), Observable.Of(2)).SwitchAll().Subscribe(onComplete: () => completed = true);

        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldForwardAnErrorFromTheActiveInnerObservable()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;

        Observable.Of(Observable.ThrowError<int>(() => error)).SwitchAll().Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }
}
