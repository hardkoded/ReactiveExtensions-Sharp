using RxSharp.Operators;

namespace RxSharp.Tests.Operators;

// Ported from rxjs 7.8.2 spec/operators/takeWhile-spec.ts (non-marble cases; other marble cases converted to
// direct-value equivalents where timing is not the point being tested).
[TestFixture]
public class TakeWhileTests
{
    [Test]
    public void ShouldTakeAllElementsUntilPredicateIsFalse()
    {
        var results = new List<int>();
        var completed = false;
        Observable.Of(2, 3, 4, 5, 6).TakeWhile(v => v < 4).Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.EqualTo(new[] { 2, 3 }));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldTakeAllElementsWithPredicateReturnsTrue()
    {
        var results = new List<string>();
        Observable.Of("b", "c", "d", "e").TakeWhile(_ => true).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { "b", "c", "d", "e" }));
    }

    [Test]
    public void ShouldSkipAllElementsWithPredicateReturnsFalse()
    {
        var results = new List<string>();
        var completed = false;
        Observable.Of("b", "c", "d", "e").TakeWhile(_ => false).Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.Empty);
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldTakeAllElementsUntilPredicateReturnFalse()
    {
        var results = new List<string>();
        Observable.Of("b", "c", "d", "e").TakeWhile(value => value != "d").Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { "b", "c" }));
    }

    [Test]
    public void ShouldTakeAllElementsUpToAndIncludingTheElementThatMadeThePredicateReturnFalse()
    {
        var results = new List<string>();
        var completed = false;
        Observable.Of("b", "c", "d", "e")
            .TakeWhile(value => value != "d", inclusive: true)
            .Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.EqualTo(new[] { "b", "c", "d" }));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldPassElementIndexToPredicate()
    {
        var results = new List<string>();
        Observable.Of("b", "c", "d", "e").TakeWhile((_, index) => index < 2).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { "b", "c" }));
    }

    [Test]
    public void ShouldRaiseErrorWhenSourceThrows()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;
        Observable.ThrowError<int>(() => error).TakeWhile(_ => true).Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldInvokePredicateUntilReturnFalse()
    {
        var invoked = 0;
        var completed = false;
        Observable.Of("b", "c", "d", "e")
            .TakeWhile(value =>
            {
                invoked++;
                return value != "d";
            })
            .Subscribe(onComplete: () => completed = true);

        Assert.That(completed, Is.True);
        Assert.That(invoked, Is.EqualTo(3));
    }

    [Test]
    public void ShouldRaiseErrorIfPredicateThrows()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;
        Observable.Of("b", "c", "d", "e").TakeWhile<string>(_ => throw error).Subscribe(onError: err => received = err);

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

        synchronousObservable.TakeWhile(value => value < 2).Subscribe(_ => { });

        Assert.That(sideEffects, Is.EqualTo(new[] { 0, 1, 2 }));
    }
}
