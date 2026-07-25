using RxSharp.Operators;
using RxSharp.Subjects;

namespace RxSharp.Tests.Operators;

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
