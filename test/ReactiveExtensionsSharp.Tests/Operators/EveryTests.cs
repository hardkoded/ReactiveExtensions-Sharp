using ReactiveExtensionsSharp.Operators;

namespace ReactiveExtensionsSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/every-spec.ts.
[TestFixture]
public class EveryTests
{
    private static bool Predicate(int x) => x % 5 == 0;

    [Test]
    public void ShouldReturnFalseIfOneElementDoesNotMatch()
    {
        var results = new List<bool>();
        Observable.Of(5, 10, 15, 18, 20).Every(Predicate).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { false }));
    }

    [Test]
    public void ShouldEmitTrueIfSourceIsEmpty()
    {
        var results = new List<bool>();
        Observable.Empty<int>().Every(Predicate).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { true }));
    }

    [Test]
    public void ShouldEmitFalseIfSingleSourceElementDoesNotMatch()
    {
        var results = new List<bool>();
        Observable.Of(3).Every(Predicate).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { false }));
    }

    [Test]
    public void ShouldEmitTrueIfSingleSourceElementMatches()
    {
        var results = new List<bool>();
        Observable.Of(5).Every(Predicate).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { true }));
    }

    [Test]
    public void ShouldEmitTrueIfAllSourceElementsMatch()
    {
        var results = new List<bool>();
        Observable.Of(5, 10, 15, 20, 25).Every(Predicate).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { true }));
    }

    [Test]
    public void ShouldIncrementIndexOnEachCallToThePredicate()
    {
        var indices = new List<int>();
        Observable.Of(1, 2, 3, 4).Every((_, i) =>
        {
            indices.Add(i);
            return true;
        }).Subscribe();

        Assert.That(indices, Is.EqualTo(new[] { 0, 1, 2, 3 }));
    }

    [Test]
    public void ShouldRaiseErrorIfSourceRaisesError()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;
        Observable.ThrowError<int>(() => error).Every(_ => true).Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldPropagateErrorIfPredicateEventuallyThrows()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;
        Observable.Of("a", "b", "c", "d").Every(x =>
        {
            if (x == "c")
            {
                throw error;
            }

            return true;
        }).Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldUnsubscribeFromTheSourceAsSoonAsThePredicateFails()
    {
        var subject = new ReactiveExtensionsSharp.Subjects.Subject<int>();
        var results = new List<bool>();
        subject.AsObservable().Every(Predicate).Subscribe(results.Add);

        subject.OnNext(5);
        subject.OnNext(3);
        subject.OnNext(10);

        Assert.That(results, Is.EqualTo(new[] { false }));
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

        synchronousObservable.Every(value => value < 2).Subscribe(_ => { });

        Assert.That(sideEffects, Is.EqualTo(new[] { 0, 1, 2 }));
    }
}
