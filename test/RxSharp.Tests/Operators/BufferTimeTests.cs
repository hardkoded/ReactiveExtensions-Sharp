using RxSharp.Operators;
using RxSharp.Subjects;

namespace RxSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/bufferTime-spec.ts.
[TestFixture]
public class BufferTimeTests
{
    [Test]
    public void ShouldEmitBuffersAtFixedIntervals()
    {
        using var signal = new ManualResetEventSlim();
        var buffers = new List<IReadOnlyList<int>>();
        var source = new Subject<int>();

        source.AsObservable().BufferTime(TimeSpan.FromMilliseconds(30)).Subscribe(buffer =>
        {
            buffers.Add(buffer);
            if (buffers.Count >= 2)
            {
                signal.Set();
            }
        });

        source.OnNext(1);
        source.OnNext(2);

        Assert.That(signal.Wait(TimeSpan.FromSeconds(2)), Is.True);
        Assert.That(buffers[0], Is.EqualTo(new[] { 1, 2 }));
    }

    [Test]
    public void ShouldEmitABufferEarlyOnceMaxBufferSizeIsReached()
    {
        var buffers = new List<IReadOnlyList<int>>();
        var source = new Subject<int>();

        source.AsObservable().BufferTime(TimeSpan.FromSeconds(5), maxBufferSize: 2).Subscribe(buffers.Add);

        source.OnNext(1);
        source.OnNext(2);
        source.OnNext(3);
        source.OnNext(4);

        Assert.That(buffers, Has.Count.EqualTo(2), "each buffer should emit as soon as it reaches maxBufferSize, without waiting for the 5s time span");
        Assert.That(buffers[0], Is.EqualTo(new[] { 1, 2 }));
        Assert.That(buffers[1], Is.EqualTo(new[] { 3, 4 }));
    }

    [Test]
    public void ShouldEmitAnEmptyTrailingBufferWhenTheSourceCompletes()
    {
        using var signal = new ManualResetEventSlim();
        var buffers = new List<IReadOnlyList<int>>();
        var completed = false;
        var source = new Subject<int>();

        source.AsObservable().BufferTime(TimeSpan.FromSeconds(5)).Subscribe(
            buffers.Add,
            onComplete: () =>
            {
                completed = true;
                signal.Set();
            });

        source.OnNext(1);
        source.OnNext(2);
        source.OnCompleted();

        Assert.That(signal.Wait(TimeSpan.FromSeconds(2)), Is.True);
        Assert.That(buffers, Has.Count.EqualTo(1));
        Assert.That(buffers[0], Is.EqualTo(new[] { 1, 2 }));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldReturnAnEmptyBufferIfSourceIsEmpty()
    {
        var buffers = new List<IReadOnlyList<int>>();
        var completed = false;

        Observable.Empty<int>().BufferTime(TimeSpan.FromSeconds(5)).Subscribe(buffers.Add, onComplete: () => completed = true);

        Assert.That(buffers, Has.Count.EqualTo(1));
        Assert.That(buffers[0], Is.Empty);
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldPropagateErrorsFromTheSourceWithoutEmittingAPendingBuffer()
    {
        var error = new InvalidOperationException("boom");
        var buffers = new List<IReadOnlyList<int>>();
        Exception? received = null;
        var source = new Subject<int>();

        source.AsObservable().BufferTime(TimeSpan.FromSeconds(5)).Subscribe(buffers.Add, err => received = err);

        source.OnNext(1);
        source.OnError(error);

        Assert.That(buffers, Is.Empty);
        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldSupportOverlappingBuffersViaACreationInterval()
    {
        using var signal = new ManualResetEventSlim();
        var opened = 0;

        Observable.Never<int>().BufferTime(TimeSpan.FromMilliseconds(60), TimeSpan.FromMilliseconds(20)).Subscribe(_ =>
        {
            if (Interlocked.Increment(ref opened) >= 3)
            {
                signal.Set();
            }
        });

        Assert.That(signal.Wait(TimeSpan.FromSeconds(2)), Is.True, "a shorter creation interval than the buffer span should open overlapping buffers");
    }
}
