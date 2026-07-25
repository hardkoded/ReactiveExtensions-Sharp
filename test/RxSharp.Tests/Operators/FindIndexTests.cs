using RxSharp.Operators;

namespace RxSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/findIndex-spec.ts.
[TestFixture]
public class FindIndexTests
{
    [Test]
    public void ShouldReturnIndexOfMatchingElementFromSourceWithSingleElement()
    {
        var results = new List<int>();
        Observable.Of(3, 9, 15, 20).FindIndex(x => x % 5 == 0).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 2 }));
    }

    [Test]
    public void ShouldReturnNegativeIndexIfSourceIsEmpty()
    {
        var results = new List<int>();
        var completed = false;
        Observable.Empty<int>().FindIndex(_ => true).Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.EqualTo(new[] { -1 }));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldReturnIndexOfMatchingElementFromSourceWithMultipleElements()
    {
        var results = new List<int>();
        Observable.Of("a", "b", "c").FindIndex(value => value == "b").Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 1 }));
    }

    [Test]
    public void ShouldReturnNegativeIndexIfElementDoesNotMatchPredicate()
    {
        var results = new List<int>();
        Observable.Of("a", "b", "c").FindIndex(value => value == "z").Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { -1 }));
    }

    [Test]
    public void ShouldSupportIndexInPredicate()
    {
        var results = new List<int>();
        Observable.Of(10, 20, 30, 40).FindIndex((_, i) => i == 2).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 2 }));
    }

    [Test]
    public void ShouldPropagateErrorFromTheSourceObservable()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;
        Observable.ThrowError<int>(() => error).FindIndex(_ => true).Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldRaiseErrorIfPredicateThrows()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;
        Observable.Of("a", "b", "c").FindIndex(_ => throw error).Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldUnsubscribeWhenThePredicateIsMatched()
    {
        var subject = new RxSharp.Subjects.Subject<string>();
        var results = new List<int>();
        subject.AsObservable().FindIndex(value => value == "b").Subscribe(results.Add);

        subject.OnNext("a");
        subject.OnNext("b");
        subject.OnNext("c");

        Assert.That(results, Is.EqualTo(new[] { 1 }));
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

        synchronousObservable.FindIndex(value => value == 2).Subscribe(_ => { });

        Assert.That(sideEffects, Is.EqualTo(new[] { 0, 1, 2 }));
    }
}
