using RxSharp.Operators;

namespace RxSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/distinct-spec.ts.
[TestFixture]
public class DistinctTests
{
    [Test]
    public void ShouldDistinguishBetweenValues()
    {
        var results = new List<string>();
        Observable.Of("a", "a", "a", "b", "b", "a").Distinct().Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { "a", "b" }));
    }

    [Test]
    public void ShouldNotOmitIfSourceElementsAreAllDifferent()
    {
        var results = new List<string>();
        Observable.Of("a", "b", "c", "d", "e", "f").Distinct().Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { "a", "b", "c", "d", "e", "f" }));
    }

    [Test]
    public void ShouldEmitOnceIfSourceElementsAreAllSame()
    {
        var results = new List<string>();
        Observable.Of("a", "a", "a", "a", "a", "a").Distinct().Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { "a" }));
    }

    [Test]
    public void ShouldCompleteIfSourceIsEmpty()
    {
        var completed = false;
        Observable.Empty<int>().Distinct().Subscribe(onComplete: () => completed = true);

        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldEmitIfSourceEmitsSingleElementOnly()
    {
        var results = new List<string>();
        var completed = false;
        Observable.Of("a").Distinct().Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.EqualTo(new[] { "a" }));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldRaiseErrorIfSourceRaisesError()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;
        Observable.ThrowError<int>(() => error).Distinct().Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldDistinguishValuesByKey()
    {
        var results = new List<int>();
        Observable.Of(1, 2, 3, 4, 5, 6).Distinct(value => value % 3).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 1, 2, 3 }));
    }

    [Test]
    public void ShouldRaiseErrorWhenKeySelectorThrows()
    {
        var results = new List<string>();
        Exception? received = null;
        Observable.Of("a", "b", "c", "d", "e").Distinct<string, string>(value =>
        {
            if (value == "d")
            {
                throw new InvalidOperationException("d is for dumb");
            }

            return value;
        }).Subscribe(results.Add, onError: err => received = err);

        Assert.That(results, Is.EqualTo(new[] { "a", "b", "c" }));
        Assert.That(received, Is.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void ShouldStopEmittingAfterExplicitUnsubscribe()
    {
        var subject = new RxSharp.Subjects.Subject<string>();
        var results = new List<string>();
        var subscription = subject.AsObservable().Distinct().Subscribe(results.Add);

        subject.OnNext("a");
        subject.OnNext("b");
        subscription.Dispose();
        subject.OnNext("c");

        Assert.That(results, Is.EqualTo(new[] { "a", "b" }));
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

        synchronousObservable.Distinct().Take(3).Subscribe(_ => { });

        Assert.That(sideEffects, Is.EqualTo(new[] { 0, 1, 2 }));
    }
}
