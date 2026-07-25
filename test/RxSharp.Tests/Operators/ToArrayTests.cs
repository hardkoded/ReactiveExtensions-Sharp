using RxSharp.Operators;

namespace RxSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/toArray-spec.ts.
[TestFixture]
public class ToArrayTests
{
    [Test]
    public void ShouldReduceTheValuesOfAnObservableIntoAList()
    {
        IReadOnlyList<string>? result = null;
        Observable.Of("a", "b").ToArray().Subscribe(value => result = value);

        Assert.That(result, Is.EqualTo(new[] { "a", "b" }));
    }

    [Test]
    public void ShouldBeEmptyWhenSourceIsEmpty()
    {
        IReadOnlyList<int>? result = null;
        var completed = false;
        Observable.Empty<int>().ToArray().Subscribe(value => result = value, onComplete: () => completed = true);

        Assert.That(result, Is.Empty);
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldWorkWithErrorWithoutEmittingAList()
    {
        var error = new InvalidOperationException("boom");
        var emitted = false;
        Exception? received = null;
        Observable.ThrowError<int>(() => error).ToArray().Subscribe(_ => emitted = true, onError: err => received = err);

        Assert.That(emitted, Is.False);
        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldNotCompleteIfSourceNeverCompletes()
    {
        var subject = new RxSharp.Subjects.Subject<int>();
        var emitted = false;
        var completed = false;
        subject.AsObservable().ToArray().Subscribe(_ => emitted = true, onComplete: () => completed = true);

        subject.OnNext(1);
        subject.OnNext(2);

        Assert.That(emitted, Is.False);
        Assert.That(completed, Is.False);
    }

    [Test]
    public void ShouldAllowMultipleIndependentSubscriptions()
    {
        var source = Observable.Of(1, 2, 3).ToArray();
        IReadOnlyList<int>? first = null;
        IReadOnlyList<int>? second = null;

        source.Subscribe(value => first = value);
        source.Subscribe(value => second = value);

        Assert.That(first, Is.EqualTo(new[] { 1, 2, 3 }));
        Assert.That(second, Is.EqualTo(new[] { 1, 2, 3 }));
    }
}
