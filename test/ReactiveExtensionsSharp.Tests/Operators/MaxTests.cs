using ReactiveExtensionsSharp.Operators;

namespace ReactiveExtensionsSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/max-spec.ts.
[TestFixture]
public class MaxTests
{
    [Test]
    public void ShouldEmitTheLargestValueUsingTheDefaultComparer()
    {
        var results = new List<int>();
        Observable.Of(5, 4, 7, 2, 8).Max().Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 8 }));
    }

    [Test]
    public void ShouldEmitTheLargestValueUsingACustomComparer()
    {
        var people = new[] { (Age: 7, Name: "Foo"), (Age: 5, Name: "Bar"), (Age: 9, Name: "Beer") };
        var results = new List<(int Age, string Name)>();

        Observable.Of(people).Max((a, b) => a.Age.CompareTo(b.Age)).Subscribe(results.Add);

        Assert.That(results.Single().Name, Is.EqualTo("Beer"));
    }

    [Test]
    public void ShouldEmitNothingForAnEmptySource()
    {
        var results = new List<int>();
        var completed = false;
        Observable.Empty<int>().Max().Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.Empty);
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldEmitTheSingleValueOfASingleElementSource()
    {
        var results = new List<int>();
        Observable.Of(42).Max().Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 42 }));
    }

    [Test]
    public void ShouldForwardErrorsWithoutEmitting()
    {
        var error = new InvalidOperationException("boom");
        var results = new List<int>();
        Exception? received = null;

        Observable.ThrowError<int>(() => error).Max().Subscribe(results.Add, onError: err => received = err);

        Assert.That(results, Is.Empty);
        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldForwardErrorThrownFromTheComparer()
    {
        var thrown = new InvalidOperationException("comparer boom");
        Exception? received = null;

        Observable.Of(1, 2).Max((_, _) => throw thrown).Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(thrown));
    }
}
