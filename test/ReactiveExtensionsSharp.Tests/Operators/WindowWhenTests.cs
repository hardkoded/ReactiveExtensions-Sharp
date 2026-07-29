using System.Reflection;
using ReactiveExtensionsSharp.Operators;
using ReactiveExtensionsSharp.Subjects;

namespace ReactiveExtensionsSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/windowWhen-spec.ts.
[TestFixture]
public class WindowWhenTests
{
    [Test]
    public void ShouldEmitWindowsThatCloseAndReopenUsingVaryingClosings()
    {
        var source = new Subject<string>();
        var closings = new List<Subject<int>>();
        var windows = new List<List<string>>();
        var index = 0;

        source.AsObservable().WindowWhen(() =>
        {
            var closing = new Subject<int>();
            closings.Add(closing);
            index++;
            return closing.AsObservable();
        }).Subscribe(window =>
        {
            var values = new List<string>();
            windows.Add(values);
            window.Subscribe(values.Add);
        });

        Assert.That(index, Is.EqualTo(1), "the first window's closing selector should be invoked eagerly at subscription time");

        source.OnNext("a");
        source.OnNext("b");
        closings[0].OnNext(0);

        source.OnNext("c");
        closings[1].OnCompleted();

        source.OnNext("d");
        source.OnCompleted();

        Assert.That(windows, Has.Count.EqualTo(3));
        Assert.That(windows[0], Is.EqualTo(new[] { "a", "b" }));
        Assert.That(windows[1], Is.EqualTo(new[] { "c" }));
        Assert.That(windows[2], Is.EqualTo(new[] { "d" }));
    }

    [Test]
    public void ShouldNotDoubleOpenAWindowWhenTheClosingNotifierEmitsThenCompletesSynchronously()
    {
        // Regression test: a naive SingleAssignmentDisposable-reassignment implementation would treat both the
        // synchronous "next" and the following synchronous "complete" from Observable.Of(0) as separate close
        // signals, opening a spurious third window instead of just reopening once. See the remarks on
        // WindowWhenOperator.WindowWhen. The second window's own closing selector returns Never so the test can
        // observe the count without the (correct, but irrelevant here) recursive-reopen behavior masking the bug.
        // Of(0) fires synchronously during the very first OpenWindow() call (at subscribe time, before any source
        // value has been pushed), so the first window closes empty, and "a" lands in the second, still-open window.
        var source = new Subject<string>();
        var windows = new List<List<string>>();
        var callCount = 0;

        source.AsObservable().WindowWhen(() =>
        {
            callCount++;
            return callCount == 1 ? Observable.Of(0) : Observable.Never<int>();
        }).Subscribe(window =>
        {
            var values = new List<string>();
            windows.Add(values);
            window.Subscribe(values.Add);
        });

        source.OnNext("a");
        source.OnCompleted();

        Assert.That(callCount, Is.EqualTo(2), "a closing notifier that emits then completes synchronously should only reopen the window once, not twice");
        Assert.That(windows, Has.Count.EqualTo(2));
        Assert.That(windows[0], Is.Empty);
        Assert.That(windows[1], Is.EqualTo(new[] { "a" }));
    }

    [Test]
    public void ShouldPropagateErrorThrownFromTheClosingSelector()
    {
        var source = new Subject<string>();
        var closings = new List<Subject<int>>();
        var error = new InvalidOperationException("boom");
        var windowErrors = new List<Exception>();
        Exception? outerError = null;
        var index = 0;

        source.AsObservable().WindowWhen(() =>
        {
            if (index == 1)
            {
                throw error;
            }

            var closing = new Subject<int>();
            closings.Add(closing);
            index++;
            return closing.AsObservable();
        }).Subscribe(
            window => window.Subscribe(onError: err => windowErrors.Add(err)),
            onError: err => outerError = err);

        source.OnNext("a");
        closings[0].OnNext(0);

        Assert.That(windowErrors, Has.Count.EqualTo(1));
        Assert.That(windowErrors[0], Is.SameAs(error));
        Assert.That(outerError, Is.SameAs(error));
    }

    [Test]
    public void ShouldPropagateAnErrorEmittedFromAClosing()
    {
        var source = new Subject<string>();
        var closing = new Subject<int>();
        var error = new InvalidOperationException("boom");
        Exception? windowError = null;
        Exception? outerError = null;

        source.AsObservable().WindowWhen(() => closing.AsObservable()).Subscribe(
            window => window.Subscribe(onError: err => windowError = err),
            onError: err => outerError = err);

        closing.OnError(error);

        Assert.That(windowError, Is.SameAs(error));
        Assert.That(outerError, Is.SameAs(error));
    }

    [Test]
    public void ShouldReturnASingleEmptyWindowForAnEmptySource()
    {
        var windows = new List<List<int>>();
        var completed = false;

        Observable.Empty<int>().WindowWhen(() => Observable.Never<int>()).Subscribe(
            window =>
            {
                var values = new List<int>();
                windows.Add(values);
                window.Subscribe(values.Add);
            },
            onComplete: () => completed = true);

        Assert.That(windows, Has.Count.EqualTo(1));
        Assert.That(windows[0], Is.Empty);
        Assert.That(completed, Is.True);
    }

    // Regression test for the disposal-cascade fix (see CLAUDE.md Learnings): the source subscription must be
    // registered as a child of the downstream subscriber (via SubscribeChild) *before* being subscribed. WindowWhen
    // opens its first window eagerly, before the source is ever subscribed and before the closing selector is
    // ever invoked, so Take(1) on the outer stream disposes right there -- the self-checking source must never
    // get to execute a single loop iteration.
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

        source.WindowWhen(() => Observable.Never<int>()).Take(1).Subscribe(_ => { });

        Assert.That(sideEffects, Is.Empty);
    }

    // Regression test for the "matching Remove on natural end" half of the same fix: each cycle's closing-notifier
    // subscriber is registered as a child of the downstream subscriber before being subscribed (so an early
    // downstream disposal can cascade into it), but it must also be *removed* again once superseded by the next
    // cycle's -- otherwise the downstream subscriber's finalizer list grows by one entry per window, forever, for
    // a long-running stream. There's no public API to observe the finalizer list, so this reaches into the
    // private field via reflection -- the only way to actually verify the growth is bounded rather than just
    // trusting that "Dispose() was called" (which was already true before this fix; only the list *entry* is new).
    private static int CountFinalizers(IDisposable subscription)
    {
        var finalizersField = typeof(Subscription).GetField("_finalizers", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var finalizers = (System.Collections.IList?)finalizersField.GetValue(subscription);
        return finalizers?.Count ?? 0;
    }

    [Test]
    public void ShouldNotAccumulateStaleClosingNotifierSubscriptionsAcrossManyWindowCycles()
    {
        int RunCycles(int cycleCount)
        {
            var trigger = new Subject<int>();
            var subscription = Observable.Never<int>()
                .WindowWhen(() => trigger.AsObservable())
                .Subscribe(window => window.Subscribe(_ => { }));

            for (var i = 0; i < cycleCount; i++)
            {
                trigger.OnNext(i);
            }

            return CountFinalizers(subscription);
        }

        // If stale closing-notifier subscriptions were never removed, the finalizer list would grow by one entry
        // per cycle, so running many more cycles would show a correspondingly larger count. Since each cycle's
        // subscription is disposed *and* removed once superseded, the count instead stays flat regardless of how
        // many windows have already opened and closed.
        Assert.That(RunCycles(25), Is.EqualTo(RunCycles(3)), "the finalizer list size should not grow with the number of completed window cycles");
    }
}
