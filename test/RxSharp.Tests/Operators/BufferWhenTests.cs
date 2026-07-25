using RxSharp.Operators;
using RxSharp.Subjects;

namespace RxSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/bufferWhen-spec.ts.
[TestFixture]
public class BufferWhenTests
{
    [Test]
    public void ShouldEmitBuffersThatCloseAndReopenUsingVaryingClosings()
    {
        var source = new Subject<string>();
        var closings = new List<Subject<int>>();
        var results = new List<IReadOnlyList<string>>();
        var index = 0;

        source.AsObservable().BufferWhen(() =>
        {
            var closing = new Subject<int>();
            closings.Add(closing);
            index++;
            return closing.AsObservable();
        }).Subscribe(results.Add);

        source.OnNext("a");
        source.OnNext("b");
        closings[0].OnNext(0);

        source.OnNext("c");
        closings[1].OnCompleted();

        source.OnNext("d");
        source.OnCompleted();

        Assert.That(results, Has.Count.EqualTo(3));
        Assert.That(results[0], Is.EqualTo(new[] { "a", "b" }));
        Assert.That(results[1], Is.EqualTo(new[] { "c" }));
        Assert.That(results[2], Is.EqualTo(new[] { "d" }));
    }

    [Test]
    public void ShouldNotDoubleEmitWhenTheClosingNotifierEmitsThenCompletesSynchronously()
    {
        // Same regression scenario as WindowWhenTests: a synchronous emit-then-complete closing notifier
        // must only close (and reopen) the buffer once, not twice. The second buffer's own closing selector
        // returns Never so the test can observe the count without the recursive-reopen behavior masking the bug.
        // Of(0) fires synchronously during the very first OpenBuffer() call (at subscribe time, before any
        // source value has been pushed), so the first (immediately closed) buffer is empty, and "a" lands in
        // the second, still-open buffer, emitted only once the source completes.
        var source = new Subject<string>();
        var results = new List<IReadOnlyList<string>>();
        var callCount = 0;

        source.AsObservable().BufferWhen(() =>
        {
            callCount++;
            return callCount == 1 ? Observable.Of(0) : Observable.Never<int>();
        }).Subscribe(results.Add);

        source.OnNext("a");
        source.OnCompleted();

        Assert.That(callCount, Is.EqualTo(2), "a closing notifier that emits then completes synchronously should only reopen the buffer once, not twice");
        Assert.That(results, Has.Count.EqualTo(2));
        Assert.That(results[0], Is.Empty);
        Assert.That(results[1], Is.EqualTo(new[] { "a" }));
    }

    [Test]
    public void ShouldEmitAnEmptyTrailingBufferOnCompletion()
    {
        var source = new Subject<string>();
        var closing = new Subject<int>();
        var results = new List<IReadOnlyList<string>>();
        var completed = false;

        source.AsObservable().BufferWhen(() => closing.AsObservable()).Subscribe(results.Add, onComplete: () => completed = true);

        source.OnNext("a");
        source.OnNext("b");
        source.OnCompleted();

        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0], Is.EqualTo(new[] { "a", "b" }));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldPropagateErrorsFromTheSource()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;
        Observable.ThrowError<int>(() => error).BufferWhen(() => Observable.Never<int>()).Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }
}
