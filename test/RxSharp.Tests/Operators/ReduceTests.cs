using RxSharp.Operators;

namespace RxSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/reduce-spec.ts.
[TestFixture]
public class ReduceTests
{
    [Test]
    public void ShouldReduceWithASeed()
    {
        var results = new List<int>();
        var completed = false;
        Observable.Of(1, 3, 5).Reduce((acc, x) => acc + x, 0).Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.EqualTo(new[] { 9 }));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldReduceWithASeedIfSourceIsEmpty()
    {
        var results = new List<string>();
        Observable.Empty<string>().Reduce((acc, x) => acc + x, "42").Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { "42" }));
    }

    [Test]
    public void ShouldReduceWithoutASeed()
    {
        var results = new List<string>();
        Observable.Of("b", "c", "d").Reduce((acc, x) => acc + " " + x).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { "b c d" }));
    }

    [Test]
    public void ShouldCompleteWithoutEmittingIfSourceDoesNotEmitWithoutSeed()
    {
        var results = new List<string>();
        var completed = false;
        Observable.Empty<string>().Reduce((acc, x) => acc + x).Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.Empty);
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldReduceWithIndexWithoutSeed()
    {
        var seenIndices = new List<int>();
        Observable.Of(0, 1, 2, 3, 4, 5).Reduce((acc, value, index) =>
        {
            seenIndices.Add(index);
            return value;
        }).Subscribe();

        Assert.That(seenIndices, Is.EqualTo(new[] { 1, 2, 3, 4, 5 }));
    }

    [Test]
    public void ShouldReduceWithIndexWithSeed()
    {
        var seenIndices = new List<int>();
        Observable.Of(0, 1, 2, 3, 4, 5).Reduce((acc, value, index) =>
        {
            seenIndices.Add(index);
            return value;
        }, -1).Subscribe();

        Assert.That(seenIndices, Is.EqualTo(new[] { 0, 1, 2, 3, 4, 5 }));
    }

    [Test]
    public void ShouldRaiseErrorIfReduceFunctionThrowsWithoutSeed()
    {
        Exception? received = null;
        Observable.Of("a", "b").Reduce<string>((_, _) => throw new InvalidOperationException("error"))
            .Subscribe(onError: err => received = err);

        Assert.That(received, Is.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void ShouldRaiseErrorIfReduceFunctionThrowsWithSeed()
    {
        Exception? received = null;
        Observable.Of("a", "b").Reduce<string, string>((_, _) => throw new InvalidOperationException("error"), "n")
            .Subscribe(onError: err => received = err);

        Assert.That(received, Is.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void ShouldRaiseErrorIfSourceRaisesErrorWithSeed()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;
        Observable.ThrowError<int>(() => error).Reduce((acc, x) => acc + x, 42).Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldNotCompleteWithSeedIfSourceNeverCompletes()
    {
        var completed = false;
        Observable.Never<int>().Reduce((acc, x) => acc + x, 0).Subscribe(onComplete: () => completed = true);

        Assert.That(completed, Is.False);
    }

    [Test]
    public void ShouldNotCompleteWithoutSeedIfSourceNeverCompletes()
    {
        var completed = false;
        Observable.Never<int>().Reduce((acc, x) => acc + x).Subscribe(onComplete: () => completed = true);

        Assert.That(completed, Is.False);
    }
}
