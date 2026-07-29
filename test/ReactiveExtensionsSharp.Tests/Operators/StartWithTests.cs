using ReactiveExtensionsSharp.Operators;

namespace ReactiveExtensionsSharp.Tests.Operators;

// Ported from rxjs 7.8.2 spec/operators/startWith-spec.ts (non-marble cases).
[TestFixture]
public class StartWithTests
{
    [Test]
    public void ShouldStartAnObservableWithGivenValue()
    {
        var results = new List<int>();
        Observable.Of(1, 2, 3).StartWith(0).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 0, 1, 2, 3 }));
    }

    [Test]
    public void ShouldStartWithGivenValuesWhenMoreThanOne()
    {
        var results = new List<int>();
        Observable.Of(3, 4).StartWith(1, 2).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 1, 2, 3, 4 }));
    }

    [Test]
    public void ShouldStartWithEmptyIfNoValuesGiven()
    {
        var results = new List<int>();
        Observable.Of(1, 2).StartWith().Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 1, 2 }));
    }

    [Test]
    public void ShouldStartWithGivenValueAndCompleteIfSourceIsEmpty()
    {
        var results = new List<int>();
        var completed = false;
        Observable.Empty<int>().StartWith(1).Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.EqualTo(new[] { 1 }));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldStartWithGivenValueAndRaiseErrorIfSourceRaisesError()
    {
        var error = new InvalidOperationException("boom");
        var results = new List<int>();
        Exception? received = null;
        Observable.ThrowError<int>(() => error).StartWith(1).Subscribe(results.Add, err => received = err);

        Assert.That(results, Is.EqualTo(new[] { 1 }));
        Assert.That(received, Is.SameAs(error));
    }
}
