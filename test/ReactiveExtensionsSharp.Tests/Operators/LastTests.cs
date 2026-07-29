using ReactiveExtensionsSharp.Operators;

namespace ReactiveExtensionsSharp.Tests.Operators;

// Ported from rxjs 7.8.2 spec/operators/last-spec.ts (all cases there are marble-based; converted to
// direct-value equivalents since timing is not the point being tested).
[TestFixture]
public class LastTests
{
    [Test]
    public void ShouldTakeTheLastValueOfAnObservable()
    {
        var results = new List<int>();
        Observable.Of(1, 2, 3).Last().Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 3 }));
    }

    [Test]
    public void ShouldErrorOnNothingSentButCompleted()
    {
        Exception? received = null;
        Observable.Empty<int>().Last().Subscribe(onError: err => received = err);

        Assert.That(received, Is.InstanceOf<EmptyErrorException>());
    }

    [Test]
    public void ShouldErrorOnEmpty()
    {
        Exception? received = null;
        Observable.Empty<int>().Last().Subscribe(onError: err => received = err);

        Assert.That(received, Is.InstanceOf<EmptyErrorException>());
    }

    [Test]
    public void ShouldAllowNullAsADefaultValue()
    {
        var results = new List<int?>();
        Observable.Of<int?>(1, 1, 1).Last(value => value == 999, null).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new int?[] { null }));
    }

    [Test]
    public void ShouldReturnLastElementMatchesWithPredicate()
    {
        var results = new List<string>();
        Observable.Of("a", "b", "a", "b").Last(value => value == "b").Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { "b" }));
    }

    [Test]
    public void ShouldReturnADefaultValueIfNoElementFound()
    {
        var results = new List<string>();
        Observable.Empty<string>().Last("a").Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { "a" }));
    }

    [Test]
    public void ShouldNotReturnDefaultValueIfAnElementIsFound()
    {
        var results = new List<string>();
        Observable.Of("b", "c", "d").Last("x").Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { "d" }));
    }

    [Test]
    public void ShouldRaiseErrorWhenPredicateThrows()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;
        Observable.Of("b", "c", "d")
            .Last(value =>
            {
                if (value == "c")
                {
                    throw error;
                }

                return false;
            })
            .Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldPropagateErrorFromTheSourceObservable()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;
        Observable.ThrowError<int>(() => error).Last().Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldUnsubscribeWhenTheSourceObservableIsUnsubscribedExplicitly()
    {
        var subject = new ReactiveExtensionsSharp.Subjects.Subject<int>();
        var results = new List<int>();
        var subscription = subject.AsObservable().Last().Subscribe(results.Add);

        subject.OnNext(1);
        subject.OnNext(2);
        subscription.Dispose();
        subject.OnNext(3);
        subject.OnCompleted();

        Assert.That(results, Is.Empty);
    }
}
