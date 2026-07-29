using ReactiveExtensionsSharp.Operators;

namespace ReactiveExtensionsSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/sequenceEqual-spec.ts.
[TestFixture]
public class SequenceEqualTests
{
    [Test]
    public void ShouldReturnTrueForTwoEqualSequences()
    {
        var results = new List<bool>();
        Observable.Of(1, 2, 3, 4, 5).SequenceEqual(Observable.Of(1, 2, 3, 4, 5)).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { true }));
    }

    [Test]
    public void ShouldReturnFalseForTwoSequencesUnequalInLength()
    {
        var results = new List<bool>();
        Observable.Of(1, 2, 3, 4, 5, 6, 7).SequenceEqual(Observable.Of(1, 2, 3)).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { false }));
    }

    [Test]
    public void ShouldReturnFalseWhenTheOtherSequenceIsLonger()
    {
        var results = new List<bool>();
        Observable.Of(1, 2, 3).SequenceEqual(Observable.Of(1, 2, 3, 4, 5, 6, 7)).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { false }));
    }

    [Test]
    public void ShouldReturnFalseForUnequalValues()
    {
        var results = new List<bool>();
        Observable.Of(1, 2, 3).SequenceEqual(Observable.Of(1, 2, 4)).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { false }));
    }

    [Test]
    public void ShouldReturnTrueForTwoEmptyObservables()
    {
        var results = new List<bool>();
        Observable.Empty<int>().SequenceEqual(Observable.Empty<int>()).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { true }));
    }

    [Test]
    public void ShouldReturnFalseIfSourceIsEmptyAndOtherIsNot()
    {
        var results = new List<bool>();
        Observable.Empty<int>().SequenceEqual(Observable.Of(1)).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { false }));
    }

    [Test]
    public void ShouldReturnFalseIfOtherIsEmptyAndSourceIsNot()
    {
        var results = new List<bool>();
        Observable.Of(1).SequenceEqual(Observable.Empty<int>()).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { false }));
    }

    [Test]
    public void ShouldErrorWithAnErroredSource()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;
        Observable.ThrowError<int>(() => error).SequenceEqual(Observable.Of(1, 2, 3)).Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldErrorWithAnErroredCompareTo()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;
        Observable.Of(1, 2, 3).SequenceEqual(Observable.ThrowError<int>(() => error)).Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldUseTheProvidedComparer()
    {
        var results = new List<bool>();
        Observable.Of(new Item("bees knees"), new Item("carpy dumb"))
            .SequenceEqual(Observable.Of(new Item("bees knees"), new Item("carpy dumb")), (a, b) => a.Value == b.Value)
            .Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { true }));
    }

    [Test]
    public void ShouldErrorIfTheComparerFunctionThrows()
    {
        var error = new InvalidOperationException("shazbot");
        Exception? received = null;
        Observable.Of(new Item("a"), new Item("b"))
            .SequenceEqual(Observable.Of(new Item("a"), new Item("b")), (_, _) => throw error)
            .Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldCompareValuesThatArriveInterleavedFromBothSides()
    {
        var source = new ReactiveExtensionsSharp.Subjects.Subject<int>();
        var other = new ReactiveExtensionsSharp.Subjects.Subject<int>();
        var results = new List<bool>();
        source.AsObservable().SequenceEqual(other.AsObservable()).Subscribe(results.Add);

        // source gets ahead of other; its values must be buffered until other catches up.
        source.OnNext(1);
        source.OnNext(2);
        other.OnNext(1);
        other.OnNext(2);
        source.OnCompleted();
        other.OnCompleted();

        Assert.That(results, Is.EqualTo(new[] { true }));
    }

    private sealed record Item(string Value);
}
