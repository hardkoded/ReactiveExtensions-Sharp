using ReactiveExtensionsSharp.Operators;
using ReactiveExtensionsSharp.Subjects;

namespace ReactiveExtensionsSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/exhaustAll-spec.ts.
[TestFixture]
public class ExhaustAllTests
{
    [Test]
    public void ShouldIgnoreInnerObservablesProducedWhileOneIsStillActive()
    {
        var a = new Subject<int>();
        var b = new Subject<int>();
        var outer = new Subject<Observable<int>>();
        var results = new List<int>();

        outer.AsObservable().ExhaustAll().Subscribe(results.Add);

        outer.OnNext(a.AsObservable());
        outer.OnNext(b.AsObservable());
        a.OnNext(1);
        b.OnNext(99);
        a.OnNext(2);

        Assert.That(results, Is.EqualTo(new[] { 1, 2 }));
    }

    [Test]
    public void ShouldAcceptANewInnerOnceThePreviousCompletes()
    {
        var results = new List<int>();
        Observable.Of(Observable.Of(1), Observable.Of(2)).ExhaustAll().Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 1, 2 }));
    }

    [Test]
    public void ShouldForwardAnErrorFromTheActiveInnerObservable()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;

        Observable.Of(Observable.ThrowError<int>(() => error)).ExhaustAll().Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }
}
