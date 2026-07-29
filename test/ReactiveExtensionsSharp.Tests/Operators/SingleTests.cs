using ReactiveExtensionsSharp.Operators;

namespace ReactiveExtensionsSharp.Tests.Operators;

// Ported from rxjs 7.8.2 spec/operators/single-spec.ts (all cases there are marble-based; converted to
// direct-value equivalents since timing is not the point being tested).
[TestFixture]
public class SingleTests
{
    [Test]
    public void ShouldRaiseErrorFromEmptyPredicateIfObservableEmitsMultipleTimes()
    {
        Exception? received = null;
        Observable.Of("a", "b", "c").Single().Subscribe(onError: err => received = err);

        Assert.That(received, Is.InstanceOf<SequenceErrorException>());
    }

    [Test]
    public void ShouldRaiseErrorFromEmptyPredicateIfObservableDoesNotEmit()
    {
        Exception? received = null;
        Observable.Empty<string>().Single().Subscribe(onError: err => received = err);

        Assert.That(received, Is.InstanceOf<EmptyErrorException>());
    }

    [Test]
    public void ShouldReturnOnlyElementFromEmptyPredicateIfObservableEmitsOnlyOnce()
    {
        var results = new List<string>();
        Observable.Of("a").Single().Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { "a" }));
    }

    [Test]
    public void ShouldRaiseErrorFromEmptyPredicateIfObservableEmitsError()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;
        Observable.ThrowError<string>(() => error).Single().Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldRaiseErrorFromPredicateIfObservableEmitsError()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;
        Observable.ThrowError<string>(() => error).Single(v => v == "c").Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldRaiseErrorIfPredicateThrowsError()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;
        Observable.Of("a", "b", "c", "d")
            .Single(v =>
            {
                if (v != "d")
                {
                    return false;
                }

                throw error;
            })
            .Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldReturnElementFromPredicateIfObservableHasSingleMatchingElement()
    {
        var results = new List<string>();
        Observable.Of("a", "b", "c").Single(v => v == "b").Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { "b" }));
    }

    [Test]
    public void ShouldRaiseErrorFromPredicateIfObservableHasMultipleMatchingElements()
    {
        Exception? received = null;
        Observable.Of("a", "b", "a", "b", "b").Single(v => v == "b").Subscribe(onError: err => received = err);

        Assert.That(received, Is.InstanceOf<SequenceErrorException>());
    }

    [Test]
    public void ShouldRaiseErrorFromPredicateIfObservableDoesNotEmit()
    {
        Exception? received = null;
        Observable.Empty<string>().Single(v => v == "a").Subscribe(onError: err => received = err);

        Assert.That(received, Is.InstanceOf<EmptyErrorException>());
    }

    [Test]
    public void ShouldRaiseErrorFromPredicateIfObservableDoesNotContainMatchingElement()
    {
        Exception? received = null;
        Observable.Of("a", "b", "c").Single(v => v == "x").Subscribe(onError: err => received = err);

        Assert.That(received, Is.InstanceOf<NotFoundErrorException>());
    }

    [Test]
    public void ShouldCallPredicateWithIndicesStartingAtZero()
    {
        var indices = new List<int>();
        var results = new List<string>();
        Observable.Of("a", "b", "c")
            .Single((v, index) =>
            {
                indices.Add(index);
                return v == "b";
            })
            .Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { "b" }));
        Assert.That(indices, Is.EqualTo(new[] { 0, 1, 2 }));
    }

    [Test]
    public void ShouldErrorForSynchronousEmptyObservablesWhenNoArgumentsAreProvided()
    {
        Exception? received = null;
        Observable.Empty<int>().Single().Subscribe(onError: err => received = err);

        Assert.That(received, Is.InstanceOf<EmptyErrorException>());
    }

    [Test]
    public void ShouldErrorForSynchronousEmptyObservablesWhenPredicateNeverPasses()
    {
        Exception? received = null;
        Observable.Empty<int>().Single(_ => false).Subscribe(onError: err => received = err);

        Assert.That(received, Is.InstanceOf<EmptyErrorException>());
    }

    [Test]
    public void ShouldErrorForSynchronousObservablesThatEmitWhenPredicateNeverPasses()
    {
        Exception? received = null;
        Observable.Of(1).Single(_ => false).Subscribe(onError: err => received = err);

        Assert.That(received, Is.InstanceOf<NotFoundErrorException>());
    }

    [Test]
    public void ShouldErrorForSynchronousObservablesWhenThePredicatePassesMoreThanOnce()
    {
        Exception? received = null;
        Observable.Of("a", "x", "b", "x", "c").Single(v => v == "x").Subscribe(onError: err => received = err);

        Assert.That(received, Is.InstanceOf<SequenceErrorException>());
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

        synchronousObservable.Single().Subscribe(_ => { }, _ => { });

        Assert.That(sideEffects, Is.EqualTo(new[] { 0, 1 }));
    }
}
