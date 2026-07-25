using RxSharp.Operators;

namespace RxSharp.Tests.Operators;

// Ported from rxjs 7.8.2 spec/operators/bufferCount-spec.ts (non-marble cases).
[TestFixture]
public class BufferCountTests
{
    [Test]
    public void ShouldBufferANumberOfValues()
    {
        var results = new List<IReadOnlyList<int>>();
        Observable.Of(1, 2, 3, 4, 5).BufferCount(2).Subscribe(results.Add);

        Assert.That(results, Has.Count.EqualTo(3));
        Assert.That(results[0], Is.EqualTo(new[] { 1, 2 }));
        Assert.That(results[1], Is.EqualTo(new[] { 3, 4 }));
        Assert.That(results[2], Is.EqualTo(new[] { 5 }), "the last, incomplete buffer is emitted on completion");
    }

    [Test]
    public void ShouldSupportOverlappingBuffersViaStartBufferEvery()
    {
        var results = new List<IReadOnlyList<int>>();
        Observable.Of(1, 2, 3, 4, 5).BufferCount(2, 1).Subscribe(results.Add);

        Assert.That(results.Select(b => b.ToArray()), Is.EqualTo(new[]
        {
            new[] { 1, 2 },
            new[] { 2, 3 },
            new[] { 3, 4 },
            new[] { 4, 5 },
            new[] { 5 },
        }));
    }

    [Test]
    public void ShouldPropagateErrorsFromTheSource()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;
        Observable.ThrowError<int>(() => error).BufferCount(2).Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }
}
