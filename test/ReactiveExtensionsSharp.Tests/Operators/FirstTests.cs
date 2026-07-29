using ReactiveExtensionsSharp.Operators;

namespace ReactiveExtensionsSharp.Tests.Operators;

// Ported from rxjs 7.8.2 spec/operators/first-spec.ts (non-marble cases).
[TestFixture]
public class FirstTests
{
    [Test]
    public void ShouldTakeTheFirstValueOfAnObservableWithManyValues()
    {
        var results = new List<int>();
        Observable.Of(1, 2, 3).First().Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 1 }));
    }

    [Test]
    public void ShouldErrorOnEmpty()
    {
        Exception? received = null;
        Observable.Empty<int>().First().Subscribe(onError: err => received = err);

        Assert.That(received, Is.InstanceOf<EmptyErrorException>());
    }

    [Test]
    public void ShouldReturnTheDefaultValueIfSourceObservableWasEmpty()
    {
        var results = new List<int>();
        var completed = false;
        Observable.Empty<int>().First(42).Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.EqualTo(new[] { 42 }));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldPropagateErrorFromTheSourceObservable()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;
        Observable.ThrowError<int>(() => error).First().Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldReturnFirstValueThatMatchesAPredicate()
    {
        var results = new List<int>();
        Observable.Of(1, 2, 3, 4, 5).First(x => x % 2 == 0).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 2 }));
    }

    [Test]
    public void ShouldErrorWhenNoValueMatchesThePredicate()
    {
        Exception? received = null;
        Observable.Of(1, 3, 5).First(x => x % 2 == 0).Subscribe(onError: err => received = err);

        Assert.That(received, Is.InstanceOf<EmptyErrorException>());
    }

    [Test]
    public void ShouldReturnTheDefaultValueWhenNoValueMatchesThePredicate()
    {
        var results = new List<int>();
        Observable.Of(1, 3, 5).First(x => x % 2 == 0, 42).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 42 }));
    }

    [Test]
    public void ShouldUnsubscribeWhenTheFirstValueIsReceived()
    {
        var subject = new ReactiveExtensionsSharp.Subjects.Subject<int>();
        var results = new List<int>();
        subject.AsObservable().First().Subscribe(results.Add);

        subject.OnNext(1);
        subject.OnNext(2);

        Assert.That(results, Is.EqualTo(new[] { 1 }));
    }

    [Test]
    public void ShouldPropagateErrorFromPredicate()
    {
        var error = new InvalidOperationException("predicate error");
        Exception? received = null;
        Observable.Of(1, 2, 3).First<int>(_ => throw error).Subscribe(onError: err => received = err);

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

        synchronousObservable.First().Subscribe(_ => { });

        Assert.That(sideEffects, Is.EqualTo(new[] { 0 }));
    }
}
