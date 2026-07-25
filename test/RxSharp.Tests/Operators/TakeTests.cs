using RxSharp.Operators;

namespace RxSharp.Tests.Operators;

// Ported from rxjs 7.8.2 spec/operators/take-spec.ts (non-marble cases).
[TestFixture]
public class TakeTests
{
    [Test]
    public void ShouldTakeTwoValuesOfAnObservableWithManyValues()
    {
        var results = new List<int>();
        var completed = false;
        Observable.Of(1, 2, 3, 4, 5).Take(2).Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.EqualTo(new[] { 1, 2 }));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldWorkWithEmpty()
    {
        var completed = false;
        Observable.Empty<int>().Take(2).Subscribe(onComplete: () => completed = true);

        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldBeEmptyOnTakeZero()
    {
        var values = new List<int>();
        var completed = false;
        Observable.Of(1, 2, 3).Take(0).Subscribe(values.Add, onComplete: () => completed = true);

        Assert.That(values, Is.Empty);
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldBeEmptyOnNegativeCount()
    {
        var values = new List<int>();
        var completed = false;
        Observable.Of(1, 2, 3).Take(-1).Subscribe(values.Add, onComplete: () => completed = true);

        Assert.That(values, Is.Empty);
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldTakeOneValueOfAnObservableWithOneValue()
    {
        var results = new List<int>();
        Observable.Of(1).Take(1).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 1 }));
    }

    [Test]
    public void ShouldPropagateErrorFromTheSourceObservable()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;
        Observable.ThrowError<int>(() => error).Take(42).Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldUnsubscribeFromTheSourceWhenItReachesTheLimit()
    {
        var subject = new RxSharp.Subjects.Subject<int>();
        var results = new List<int>();
        subject.AsObservable().Take(2).Subscribe(results.Add);

        subject.OnNext(1);
        subject.OnNext(2);
        subject.OnNext(3);

        Assert.That(results, Is.EqualTo(new[] { 1, 2 }));
    }

    [Test]
    public void ShouldCompleteWhenTheSourceIsReentrant()
    {
        var subject = new RxSharp.Subjects.Subject<int>();
        var results = new List<int>();
        var reentrantCallCount = 0;

        subject.AsObservable().Take(1).Subscribe(value =>
        {
            results.Add(value);
            reentrantCallCount++;
            if (reentrantCallCount == 1)
            {
                subject.OnNext(2);
            }
        });

        subject.OnNext(1);

        Assert.That(results, Is.EqualTo(new[] { 1 }));
    }

    [Test]
    public void ShouldStopListeningToASynchronousObservableWhenUnsubscribed()
    {
        var sideEffects = new List<int>();
        var synchronousObservable = new Observable<int>(subscriber =>
        {
            // This checks whether the subscriber was closed on each loop; when the unsubscribe hits
            // (from Take), it should be closed, and the loop should stop even mid-emission.
            for (var i = 0; !subscriber.IsDisposed && i < 10; i++)
            {
                sideEffects.Add(i);
                subscriber.OnNext(i);
            }
        });

        synchronousObservable.Take(3).Subscribe(_ => { });

        Assert.That(sideEffects, Is.EqualTo(new[] { 0, 1, 2 }));
    }
}
