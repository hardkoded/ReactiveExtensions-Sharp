using RxSharp.Operators;
using RxSharp.Subjects;

namespace RxSharp.Tests.Operators;

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
}
