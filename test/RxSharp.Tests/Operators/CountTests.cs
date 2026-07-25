using RxSharp.Operators;

namespace RxSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/count-spec.ts.
[TestFixture]
public class CountTests
{
    [Test]
    public void ShouldCountTheValuesOfAnObservable()
    {
        var results = new List<int>();
        Observable.Of("a", "b", "c").Count().Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 3 }));
    }

    [Test]
    public void ShouldBeZeroWhenSourceIsEmpty()
    {
        var results = new List<int>();
        var completed = false;
        Observable.Empty<int>().Count().Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.EqualTo(new[] { 0 }));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldCountValuesMatchingAPredicate()
    {
        var results = new List<int>();
        Observable.Of(2, 4, 6, 7, 8).Count(x => x % 2 == 0).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 4 }));
    }

    [Test]
    public void ShouldHandleAnAlwaysTruePredicateOnAnEmptyObservable()
    {
        var results = new List<int>();
        Observable.Empty<int>().Count(_ => true).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 0 }));
    }

    [Test]
    public void ShouldHandleAnAlwaysFalsePredicateOnObservableWithManyValues()
    {
        var results = new List<int>();
        Observable.Of(1, 2, 3).Count(_ => false).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 0 }));
    }

    [Test]
    public void ShouldPropagateErrorFromTheSourceObservable()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;
        Observable.ThrowError<int>(() => error).Count().Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldHandleAPredicateThatThrows()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;
        Observable.Of(1, 2, 3).Count(x =>
        {
            if (x == 3)
            {
                throw error;
            }

            return true;
        }).Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldNotCompleteIfSourceNeverCompletes()
    {
        var subject = new RxSharp.Subjects.Subject<int>();
        var results = new List<int>();
        var completed = false;
        subject.AsObservable().Count().Subscribe(results.Add, onComplete: () => completed = true);

        subject.OnNext(1);
        subject.OnNext(2);

        Assert.That(results, Is.Empty);
        Assert.That(completed, Is.False);
    }
}
