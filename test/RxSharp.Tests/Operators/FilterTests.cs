using RxSharp.Operators;

namespace RxSharp.Tests.Operators;

// Ported from rxjs 7.8.2 spec/operators/filter-spec.ts (non-marble cases).
[TestFixture]
public class FilterTests
{
    [Test]
    public void ShouldFilterOutEvenValues()
    {
        var results = new List<int>();
        Observable.Of(1, 2, 3, 4, 5).Filter(x => x % 2 == 1).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 1, 3, 5 }));
    }

    [Test]
    public void ShouldFilterWithAnAlwaysTruePredicate()
    {
        var results = new List<int>();
        Observable.Of(1, 2, 3).Filter(_ => true).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 1, 2, 3 }));
    }

    [Test]
    public void ShouldFilterWithAnAlwaysFalsePredicate()
    {
        var results = new List<int>();
        Observable.Of(1, 2, 3).Filter(_ => false).Subscribe(results.Add);

        Assert.That(results, Is.Empty);
    }

    [Test]
    public void ShouldInvokePredicateOnceForEachCheckedValue()
    {
        var invokeCount = 0;
        Observable.Of(1, 2, 3).Filter(_ => { invokeCount++; return true; }).Subscribe();

        Assert.That(invokeCount, Is.EqualTo(3));
    }

    [Test]
    public void ShouldSupportPredicateWithIndex()
    {
        var results = new List<int>();
        Observable.Of(10, 20, 30).Filter((_, i) => i % 2 == 0).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 10, 30 }));
    }

    [Test]
    public void ShouldPropagateErrorsFromTheSource()
    {
        var error = new InvalidOperationException("source error");
        Exception? received = null;
        Observable.ThrowError<int>(() => error).Filter(_ => true).Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldSendErrorsDownTheErrorPathWhenPredicateThrows()
    {
        var error = new InvalidOperationException("predicate error");
        Exception? received = null;
        Observable.Of(1, 2, 3).Filter<int>(_ => throw error).Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldComposeWithAnotherFilterToAllowMultiplesOfSix()
    {
        var results = new List<int>();
        Observable.Of(1, 2, 3, 4, 5, 6, 12).Filter(x => x % 2 == 0).Filter(x => x % 3 == 0).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 6, 12 }));
    }
}
