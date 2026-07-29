using System.Reflection;
using ReactiveExtensionsSharp.Operators;
using ReactiveExtensionsSharp.Subjects;

namespace ReactiveExtensionsSharp.Tests.Operators;

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

    // Regression test for the disposal-cascade fix (see CLAUDE.md Learnings): the source subscription must be
    // registered as a child of the downstream subscriber (via SubscribeChild) *before* being subscribed. Unlike
    // WindowWhen, BufferWhen does not emit eagerly on the very first cycle (a buffer only appears once it closes),
    // but a closing selector that fires synchronously (Observable.Of(0)) causes a recursive second cycle whose
    // *first* buffer (the empty one from cycle 1) is emitted before the source is ever subscribed -- so Take(1)
    // on the outer stream still disposes before src.SubscribeChild(...) ever runs, and the self-checking source
    // must never get to execute a single loop iteration.
    [Test]
    public void ShouldCascadeDisposalToTheSourceBeforeItIsEverSubscribed()
    {
        var sideEffects = new List<int>();
        Observable<int> source = new(subscriber =>
        {
            for (var i = 0; !subscriber.IsDisposed && i < 10; i++)
            {
                sideEffects.Add(i);
                subscriber.OnNext(i);
            }
        });

        source.BufferWhen(() => Observable.Of(0)).Take(1).Subscribe(_ => { });

        Assert.That(sideEffects, Is.Empty);
    }

    // Regression test for the "matching Remove on natural end" half of the same fix: each cycle's closing-notifier
    // subscriber is registered as a child of the downstream subscriber before being subscribed, but it must also
    // be *removed* again once superseded, or the downstream subscriber's finalizer list grows by one entry per
    // buffer, forever, for a long-running stream. There's no public API to observe the finalizer list, so this
    // reaches into the private field via reflection and compares the count after few vs. many cycles -- if stale
    // subscriptions were never removed, running more cycles would show a correspondingly larger count.
    [Test]
    public void ShouldNotAccumulateStaleClosingNotifierSubscriptionsAcrossManyBufferCycles()
    {
        int RunCycles(int cycleCount)
        {
            var trigger = new Subject<int>();
            var subscription = Observable.Never<int>()
                .BufferWhen(() => trigger.AsObservable())
                .Subscribe(_ => { });

            for (var i = 0; i < cycleCount; i++)
            {
                trigger.OnNext(i);
            }

            var finalizersField = typeof(Subscription).GetField("_finalizers", BindingFlags.NonPublic | BindingFlags.Instance)!;
            var finalizers = (System.Collections.IList?)finalizersField.GetValue(subscription);
            return finalizers?.Count ?? 0;
        }

        Assert.That(RunCycles(25), Is.EqualTo(RunCycles(3)), "the finalizer list size should not grow with the number of completed buffer cycles");
    }
}
