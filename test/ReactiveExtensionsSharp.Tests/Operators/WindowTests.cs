using ReactiveExtensionsSharp.Operators;
using ReactiveExtensionsSharp.Subjects;

namespace ReactiveExtensionsSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/window-spec.ts.
[TestFixture]
public class WindowTests
{
    [Test]
    public void ShouldEmitWindowsThatCloseAndReopenOnEachBoundaryEmission()
    {
        var source = new Subject<string>();
        var closings = new Subject<int>();
        var windows = new List<List<string>>();

        source.AsObservable().Window(closings.AsObservable()).Subscribe(window =>
        {
            var values = new List<string>();
            windows.Add(values);
            window.Subscribe(values.Add);
        });

        source.OnNext("a");
        source.OnNext("b");
        closings.OnNext(0);
        source.OnNext("c");
        source.OnNext("d");
        closings.OnNext(0);
        source.OnNext("e");
        source.OnCompleted();

        Assert.That(windows, Has.Count.EqualTo(3));
        Assert.That(windows[0], Is.EqualTo(new[] { "a", "b" }));
        Assert.That(windows[1], Is.EqualTo(new[] { "c", "d" }));
        Assert.That(windows[2], Is.EqualTo(new[] { "e" }));
    }

    [Test]
    public void ShouldReturnASingleEmptyWindowIfSourceIsEmpty()
    {
        var windows = new List<List<int>>();
        var completed = false;

        Observable.Empty<int>().Window(Observable.Empty<int>()).Subscribe(
            window =>
            {
                var values = new List<int>();
                window.Subscribe(values.Add);
                windows.Add(values);
            },
            onComplete: () => completed = true);

        Assert.That(windows, Has.Count.EqualTo(1));
        Assert.That(windows[0], Is.Empty);
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldEmitAnErrorOnlyWindowIfSourceThrows()
    {
        var error = new InvalidOperationException("boom");
        Exception? windowError = null;
        Exception? outerError = null;

        Observable.ThrowError<int>(() => error).Window(Observable.Never<int>()).Subscribe(
            window => window.Subscribe(onError: err => windowError = err),
            onError: err => outerError = err);

        Assert.That(windowError, Is.SameAs(error));
        Assert.That(outerError, Is.SameAs(error));
    }

    [Test]
    public void ShouldMakeTheOuterEmitAnErrorWhenTheBoundaryErrors()
    {
        var source = new Subject<int>();
        var closings = new Subject<int>();
        var error = new InvalidOperationException("boom");
        Exception? windowError = null;
        Exception? outerError = null;

        source.AsObservable().Window(closings.AsObservable()).Subscribe(
            window => window.Subscribe(onError: err => windowError = err),
            onError: err => outerError = err);

        source.OnNext(1);
        closings.OnError(error);

        Assert.That(windowError, Is.SameAs(error));
        Assert.That(outerError, Is.SameAs(error));
    }

    // Regression tests for the disposal-cascade fix (see CLAUDE.md Learnings): both the source subscription and the
    // boundary subscription are single-stable-for-the-operator's-lifetime, so both must be registered as children
    // of the downstream subscriber via SubscribeChild *before* being subscribed, not only after Subscribe returns.
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
    public void ShouldCascadeDisposalToTheSourceBeforeItIsEverSubscribed()
    {
        // Window emits its first window eagerly, before the source is ever subscribed. Take(1) on the outer
        // stream therefore disposes the downstream subscriber right there -- before src.SubscribeChild(...) even
        // runs -- so the self-checking source must never get to execute a single loop iteration.
        var sideEffects = new List<int>();

        SynchronousObservable(sideEffects).Window(Observable.Never<int>()).Take(1).Subscribe(_ => { });

        Assert.That(sideEffects, Is.Empty);
    }

    [Test]
    public void ShouldCascadeDisposalToTheActiveBoundarySubscription()
    {
        // Use the self-checking source as the *boundary* instead, so its loop runs synchronously while windows
        // are actively being opened. Take(3) on the outer stream counts the eager first window (opened before
        // the boundary is even subscribed) plus two boundary-driven reopens, and must cascade into stopping the
        // boundary's own loop mid-iteration.
        var sideEffects = new List<int>();

        Observable.Never<int>().Window(SynchronousObservable(sideEffects)).Take(3).Subscribe(_ => { });

        Assert.That(sideEffects, Is.EqualTo(new[] { 0, 1 }));
    }

    [Test]
    public void ShouldCompleteTheResultingObservableWhenBoundaryCompletes()
    {
        var source = new Subject<int>();
        var closings = new Subject<int>();
        var windows = new List<List<int>>();

        source.AsObservable().Window(closings.AsObservable()).Subscribe(window =>
        {
            var values = new List<int>();
            windows.Add(values);
            window.Subscribe(values.Add);
        });

        source.OnNext(1);
        closings.OnNext(0);
        source.OnNext(2);
        closings.OnCompleted();
        source.OnNext(3);

        Assert.That(windows, Has.Count.EqualTo(2), "the boundary completing shouldn't open a new window or close the current one");
        Assert.That(windows[0], Is.EqualTo(new[] { 1 }));
        Assert.That(windows[1], Is.EqualTo(new[] { 2, 3 }));
    }
}
