using RxSharp.Operators;

namespace RxSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/find-spec.ts.
[TestFixture]
public class FindTests
{
    [Test]
    public void ShouldReturnMatchingElementFromSourceWithSingleElement()
    {
        var results = new List<int>();
        Observable.Of(3, 9, 15, 20).Find(x => x % 5 == 0).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 15 }));
    }

    [Test]
    public void ShouldReturnDefaultIfSourceIsEmpty()
    {
        var results = new List<string?>();
        var completed = false;
        Observable.Empty<string>().Find(_ => true).Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.EqualTo(new string?[] { null }));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldReturnMatchingElementFromSourceWithMultipleElements()
    {
        var results = new List<string?>();
        Observable.Of("a", "b", "c").Find(value => value == "b").Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { "b" }));
    }

    [Test]
    public void ShouldReturnDefaultIfElementDoesNotMatchPredicate()
    {
        var results = new List<string?>();
        Observable.Of("a", "b", "c").Find(value => value == "z").Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new string?[] { null }));
    }

    [Test]
    public void ShouldSupportIndexInPredicate()
    {
        var results = new List<int>();
        Observable.Of(10, 20, 30, 40).Find((_, i) => i == 2).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 30 }));
    }

    [Test]
    public void ShouldPropagateErrorFromTheSourceObservable()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;
        Observable.ThrowError<int>(() => error).Find(_ => true).Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldRaiseErrorIfPredicateThrows()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;
        Observable.Of("a", "b", "c").Find<string>(_ => throw error).Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldUnsubscribeWhenThePredicateIsMatched()
    {
        var subject = new RxSharp.Subjects.Subject<string>();
        var results = new List<string?>();
        subject.AsObservable().Find(value => value == "b").Subscribe(results.Add);

        subject.OnNext("a");
        subject.OnNext("b");
        subject.OnNext("c");

        Assert.That(results, Is.EqualTo(new[] { "b" }));
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

        synchronousObservable.Find(value => value == 2).Subscribe(_ => { });

        Assert.That(sideEffects, Is.EqualTo(new[] { 0, 1, 2 }));
    }
}
