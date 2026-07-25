using RxSharp.Operators;

namespace RxSharp.Tests.Operators;

// Ported from rxjs 7.8.2 spec/operators/map-spec.ts (non-marble cases).
[TestFixture]
public class MapTests
{
    [Test]
    public void ShouldMapMultipleValues()
    {
        var results = new List<int>();
        Observable.Of(1, 2, 3).Map(x => x * 10).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 10, 20, 30 }));
    }

    [Test]
    public void ShouldMapOneValue()
    {
        var results = new List<int>();
        Observable.Of(1).Map(x => x * 10).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 10 }));
    }

    [Test]
    public void ShouldPropagateErrorsFromMapFunction()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;
        Observable.Of(1, 2, 3)
            .Map<int, int>(_ => throw error)
            .Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldNotMapAnEmptyObservable()
    {
        var called = false;
        var completed = false;
        Observable.Empty<int>().Map(x => { called = true; return x; }).Subscribe(onComplete: () => completed = true);

        Assert.That(called, Is.False);
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldMapWithIndex()
    {
        var results = new List<int>();
        Observable.Of(10, 20, 30).Map((x, i) => x + i).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 10, 21, 32 }));
    }

    [Test]
    public void ShouldMapTwice()
    {
        var results = new List<int>();
        Observable.Of(1, 2, 3).Map(x => x + 1).Map(x => x * 10).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 20, 30, 40 }));
    }

    [Test]
    public void ShouldStopMappingAfterUnsubscribing()
    {
        var results = new List<int>();
        var subscription = Observable.Of(1, 2, 3).Map(x => x).Subscribe(results.Add);
        subscription.Dispose();

        // Of/From emit synchronously and complete before Subscribe returns, so this
        // only verifies unsubscribing after completion doesn't throw or duplicate values.
        Assert.That(results, Is.EqualTo(new[] { 1, 2, 3 }));
    }
}
