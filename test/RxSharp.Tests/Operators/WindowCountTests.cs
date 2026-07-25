using RxSharp.Operators;

namespace RxSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/windowCount-spec.ts.
[TestFixture]
public class WindowCountTests
{
    [Test]
    public void ShouldEmitWindowsWithCount3NoSkipSpecified()
    {
        var windows = new List<List<int>>();
        var completed = false;

        Observable.Of(1, 2, 3, 4, 5, 6, 7, 8, 9).WindowCount(3).Subscribe(
            window =>
            {
                var values = new List<int>();
                window.Subscribe(values.Add);
                windows.Add(values);
            },
            onComplete: () => completed = true);

        Assert.That(windows, Has.Count.EqualTo(4), "3 full windows plus a trailing empty window opened right after the 9th value");
        Assert.That(windows[0], Is.EqualTo(new[] { 1, 2, 3 }));
        Assert.That(windows[1], Is.EqualTo(new[] { 4, 5, 6 }));
        Assert.That(windows[2], Is.EqualTo(new[] { 7, 8, 9 }));
        Assert.That(windows[3], Is.Empty, "the trailing window never receives a value before the source completes");
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldEmitOverlappingWindowsWithCount2AndSkip1()
    {
        var windows = new List<List<int>>();

        Observable.Of(1, 2, 3, 4).WindowCount(2, 1).Subscribe(window =>
        {
            var values = new List<int>();
            window.Subscribe(values.Add);
            windows.Add(values);
        });

        Assert.That(windows, Has.Count.EqualTo(5));
        Assert.That(windows[0], Is.EqualTo(new[] { 1, 2 }));
        Assert.That(windows[1], Is.EqualTo(new[] { 2, 3 }));
        Assert.That(windows[2], Is.EqualTo(new[] { 3, 4 }));
        Assert.That(windows[3], Is.EqualTo(new[] { 4 }));
        Assert.That(windows[4], Is.Empty);
    }

    [Test]
    public void ShouldReturnASingleEmptyWindowIfSourceIsEmpty()
    {
        var windows = new List<List<int>>();
        var completed = false;

        Observable.Empty<int>().WindowCount(2, 1).Subscribe(
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
    public void ShouldPropagateErrorFromAJustThrowSource()
    {
        var error = new InvalidOperationException("boom");
        Exception? windowError = null;
        Exception? outerError = null;

        Observable.ThrowError<int>(() => error).WindowCount(2, 1).Subscribe(
            window => window.Subscribe(onError: err => windowError = err),
            onError: err => outerError = err);

        Assert.That(windowError, Is.SameAs(error));
        Assert.That(outerError, Is.SameAs(error));
    }

    // rxjs's "should stop listening to a synchronous observable when unsubscribed" test is not ported here:
    // it relies on a `Take`-driven synchronous unsubscribe interrupting a still-running upstream `Subscribe`
    // call, which this port's `SingleAssignmentDisposable`-based teardown does not support yet (the disposal
    // only takes effect once the nested synchronous `Subscribe` call unwinds, i.e. after it already ran to
    // completion) — a pre-existing gap in `Take` itself, reproducible with `Take` alone, not specific to `WindowCount`.
}
