using RxSharp.Operators;

namespace RxSharp.Tests.Operators;

// Ported from rxjs 7.8.2 spec/operators/takeLast-spec.ts (all cases there are marble-based; converted to
// direct-value equivalents since timing is not the point being tested).
[TestFixture]
public class TakeLastTests
{
    [Test]
    public void ShouldTakeTwoValuesOfAnObservableWithManyValues()
    {
        var results = new List<int>();
        var completed = false;
        Observable.Of(1, 2, 3, 4).TakeLast(2).Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.EqualTo(new[] { 3, 4 }));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldTakeLastThreeValues()
    {
        var results = new List<int>();
        Observable.Of(1, 2, 3, 4).TakeLast(3).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 2, 3, 4 }));
    }

    [Test]
    public void ShouldTakeAllElementWhenTryToTakeLargerThenSource()
    {
        var results = new List<int>();
        Observable.Of(1, 2, 3, 4).TakeLast(5).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 1, 2, 3, 4 }));
    }

    [Test]
    public void ShouldTakeAllElementWhenTryToTakeExact()
    {
        var results = new List<int>();
        Observable.Of(1, 2, 3, 4).TakeLast(4).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 1, 2, 3, 4 }));
    }

    [Test]
    public void ShouldNotTakeAnyValues()
    {
        var results = new List<int>();
        var completed = false;
        Observable.Of(1, 2, 3, 4).TakeLast(0).Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.Empty);
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldNotTakeAnyValuesIfProvidedWithNegativeValue()
    {
        var results = new List<int>();
        var completed = false;
        Observable.Of(1, 2, 3, 4).TakeLast(-42).Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.Empty);
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldWorkWithEmpty()
    {
        var completed = false;
        Observable.Empty<int>().TakeLast(42).Subscribe(onComplete: () => completed = true);

        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldTakeOneValueFromAnObservableWithOneValue()
    {
        var results = new List<int>();
        Observable.Of(1).TakeLast(1).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 1 }));
    }

    [Test]
    public void ShouldTakeOneValueFromAnObservableWithManyValues()
    {
        var results = new List<int>();
        Observable.Of(1, 2, 3, 4).TakeLast(1).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 4 }));
    }

    [Test]
    public void ShouldCompleteWithoutEmittingOnEmptySource()
    {
        var results = new List<int>();
        var completed = false;
        Observable.Empty<int>().TakeLast(42).Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.Empty);
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldPropagateErrorFromTheSourceObservable()
    {
        var error = new InvalidOperationException("too bad");
        Exception? received = null;
        Observable.ThrowError<int>(() => error).TakeLast(42).Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldPropagateErrorFromAnObservableWithValues()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;
        var results = new List<int>();
        Observable.Concat(Observable.Of(1, 2), Observable.ThrowError<int>(() => error))
            .TakeLast(42)
            .Subscribe(results.Add, onError: err => received = err);

        Assert.That(results, Is.Empty);
        Assert.That(received, Is.SameAs(error));
    }
}
