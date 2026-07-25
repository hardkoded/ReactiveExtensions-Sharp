using RxSharp.Operators;

namespace RxSharp.Tests.Operators;

// Ported from rxjs 7.8.2 spec/operators/throwIfEmpty-spec.ts (non-marble cases).
[TestFixture]
public class ThrowIfEmptyTests
{
    [Test]
    public void ShouldErrorWithEmptyErrorWhenSourceIsEmpty()
    {
        Exception? received = null;
        Observable.Empty<int>().ThrowIfEmpty().Subscribe(onError: err => received = err);

        Assert.That(received, Is.InstanceOf<EmptyErrorException>());
    }

    [Test]
    public void ShouldUseTheCustomErrorFactory()
    {
        Exception? received = null;
        Observable.Empty<int>()
            .ThrowIfEmpty(() => new InvalidOperationException("The document was not clicked within 1 second"))
            .Subscribe(onError: err => received = err);

        Assert.That(received, Is.InstanceOf<InvalidOperationException>());
        Assert.That(received!.Message, Is.EqualTo("The document was not clicked within 1 second"));
    }

    [Test]
    public void ShouldMirrorTheSourceWhenItEmitsValues()
    {
        var results = new List<int>();
        var completed = false;
        Observable.Of(1, 2, 3).ThrowIfEmpty().Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.EqualTo(new[] { 1, 2, 3 }));
        Assert.That(completed, Is.True);
    }
}
