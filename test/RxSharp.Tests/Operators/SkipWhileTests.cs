using RxSharp.Operators;

namespace RxSharp.Tests.Operators;

// Ported from rxjs 7.8.2 spec/operators/skipWhile-spec.ts (non-marble cases; other marble cases converted to
// direct-value equivalents where timing is not the point being tested).
[TestFixture]
public class SkipWhileTests
{
    [Test]
    public void ShouldSkipAllElementsUntilPredicateIsFalse()
    {
        var results = new List<int>();
        Observable.Of(2, 3, 4, 5, 6).SkipWhile(v => v < 4).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 4, 5, 6 }));
    }

    [Test]
    public void ShouldSkipAllElementsWithATruePredicate()
    {
        var results = new List<int>();
        Observable.Of(2, 3, 4, 5, 6).SkipWhile(_ => true).Subscribe(results.Add);

        Assert.That(results, Is.Empty);
    }

    [Test]
    public void ShouldNotSkipAnyElementWithAFalsePredicate()
    {
        var results = new List<int>();
        Observable.Of(2, 3, 4, 5, 6).SkipWhile(_ => false).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 2, 3, 4, 5, 6 }));
    }

    [Test]
    public void ShouldBePossibleToSkipUsingTheElementsIndex()
    {
        var results = new List<string>();
        Observable.Of("c", "d", "e", "f", "g", "h").SkipWhile((_, index) => index < 2).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { "e", "f", "g", "h" }));
    }

    [Test]
    public void ShouldSkipUsingValueWithSourceThrows()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;
        var results = new List<string>();
        Observable.Concat(Observable.Of("c", "d", "e", "f", "g", "h"), Observable.ThrowError<string>(() => error))
            .SkipWhile(v => v != "d")
            .Subscribe(results.Add, onError: err => received = err);

        Assert.That(results, Is.EqualTo(new[] { "d", "e", "f", "g", "h" }));
        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldInvokePredicateWhileItsFalseAndNeverAgain()
    {
        var invoked = 0;
        var completed = false;
        Observable.Of("c", "d", "e", "f", "g", "h")
            .SkipWhile(v =>
            {
                invoked++;
                return v != "e";
            })
            .Subscribe(onComplete: () => completed = true);

        Assert.That(completed, Is.True);
        Assert.That(invoked, Is.EqualTo(3));
    }

    [Test]
    public void ShouldHandlePredicateThatThrows()
    {
        var error = new InvalidOperationException("nom d'une pipe !");
        Exception? received = null;
        Observable.Of("c", "d", "e", "f", "g", "h")
            .SkipWhile(v =>
            {
                if (v == "e")
                {
                    throw error;
                }

                return v != "f";
            })
            .Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldHandleObservableEmpty()
    {
        var completed = false;
        Observable.Empty<int>().SkipWhile(_ => true).Subscribe(onComplete: () => completed = true);

        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldHandleObservableThrow()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;
        Observable.ThrowError<int>(() => error).SkipWhile(_ => true).Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldStopListeningToASynchronousObservableWhenUnsubscribed()
    {
        var sideEffects = new List<int>();
        var synchronousObservable = new Observable<int>(subscriber =>
        {
            for (var i = 0; !subscriber.IsDisposed && i < 10; i++)
            {
                sideEffects.Add(i);
                subscriber.OnNext(i);
            }
        });

        synchronousObservable.SkipWhile(value => value < 2).Take(1).Subscribe(_ => { });

        Assert.That(sideEffects, Is.EqualTo(new[] { 0, 1, 2 }));
    }
}
