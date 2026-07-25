using RxSharp.Operators;
using RxSharp.Subjects;

namespace RxSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/buffer-spec.ts.
[TestFixture]
public class BufferTests
{
    [Test]
    public void ShouldEmitBuffersThatCloseAndReopenOnEachClosingNotifierEmission()
    {
        var source = new Subject<string>();
        var closings = new Subject<int>();
        var results = new List<IReadOnlyList<string>>();

        source.AsObservable().Buffer(closings.AsObservable()).Subscribe(results.Add);

        source.OnNext("a");
        source.OnNext("b");
        source.OnNext("c");
        closings.OnNext(0);
        source.OnNext("d");
        source.OnNext("e");
        source.OnNext("f");
        closings.OnNext(0);
        source.OnNext("g");
        source.OnNext("h");
        source.OnNext("i");
        source.OnCompleted();

        Assert.That(results, Has.Count.EqualTo(3));
        Assert.That(results[0], Is.EqualTo(new[] { "a", "b", "c" }));
        Assert.That(results[1], Is.EqualTo(new[] { "d", "e", "f" }));
        Assert.That(results[2], Is.EqualTo(new[] { "g", "h", "i" }));
    }

    [Test]
    public void ShouldEmitAllBufferedValuesIfTheSourceCompletesBeforeTheClosingNotifierDoes()
    {
        var source = new Subject<string>();
        var closer = new Subject<int>();
        var results = new List<IReadOnlyList<string>>();
        var completed = false;

        source.AsObservable().Buffer(closer.AsObservable()).Subscribe(results.Add, onComplete: () => completed = true);

        source.OnNext("a");
        source.OnNext("b");
        source.OnNext("c");
        closer.OnNext(0);
        source.OnNext("d");
        source.OnNext("e");
        source.OnNext("f");
        source.OnCompleted();

        Assert.That(results, Has.Count.EqualTo(2));
        Assert.That(results[0], Is.EqualTo(new[] { "a", "b", "c" }));
        Assert.That(results[1], Is.EqualTo(new[] { "d", "e", "f" }));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldWorkWithEmptySourceAndEmptyClosingNotifier()
    {
        var results = new List<IReadOnlyList<int>>();
        Observable.Empty<int>().Buffer(Observable.Empty<int>()).Subscribe(results.Add);

        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0], Is.Empty);
    }

    [Test]
    public void ShouldPropagateErrorsFromTheSource()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;
        Observable.ThrowError<int>(() => error).Buffer(Observable.Never<int>()).Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }

    // Regression test for the disposal-cascade fix (see CLAUDE.md Learnings): both the source subscription and the
    // closing-notifier subscription are single-stable-for-the-operator's-lifetime, so both must be registered as
    // children of the downstream subscriber via SubscribeChild *before* being subscribed. Buffer never emits
    // eagerly (unlike Window, a buffer only appears once it closes), so the only way to exercise a synchronous,
    // self-checking source's mid-loop stop here is to put it in the *notifier* role: each of its values closes
    // (and emits) the current buffer, so the notifier's own loop runs live while buffers are being produced.
    private static Observable<int> SynchronousObservable(List<int> sideEffects)
        => new(subscriber =>
        {
            for (var i = 0; !subscriber.IsDisposed && i < 10; i++)
            {
                sideEffects.Add(i);
                subscriber.OnNext(i);
            }
        });

    [Test]
    public void ShouldCascadeDisposalToTheActiveClosingNotifierSubscription()
    {
        var sideEffects = new List<int>();

        Observable.Never<int>().Buffer(SynchronousObservable(sideEffects)).Take(3).Subscribe(_ => { });

        Assert.That(sideEffects, Is.EqualTo(new[] { 0, 1, 2 }));
    }
}
