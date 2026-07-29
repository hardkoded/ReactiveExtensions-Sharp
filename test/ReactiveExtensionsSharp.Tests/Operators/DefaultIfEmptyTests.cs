using ReactiveExtensionsSharp.Operators;

namespace ReactiveExtensionsSharp.Tests.Operators;

// Ported from rxjs 7.8.2 spec/operators/defaultIfEmpty-spec.ts (non-marble cases).
[TestFixture]
public class DefaultIfEmptyTests
{
    [Test]
    public void ShouldEmitTheDefaultValueWhenSourceIsEmpty()
    {
        var results = new List<string>();
        Observable.Empty<string>().DefaultIfEmpty("no clicks").Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { "no clicks" }));
    }

    [Test]
    public void ShouldMirrorTheSourceWhenItEmitsValues()
    {
        var results = new List<int>();
        Observable.Of(1, 2, 3).DefaultIfEmpty(-1).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 1, 2, 3 }));
    }

    [Test]
    public void ShouldPropagateErrorsFromTheSource()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;
        Observable.ThrowError<int>(() => error).DefaultIfEmpty(-1).Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }
}
