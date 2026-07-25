using RxSharp.Operators;

namespace RxSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/windowTime-spec.ts.
[TestFixture]
public class WindowTimeTests
{
    [Test]
    public void ShouldEmitNonOverlappingWindowsGivenOnlyAWindowTimeSpan()
    {
        using var signal = new ManualResetEventSlim();
        var windows = new List<List<int>>();
        var source = new RxSharp.Subjects.Subject<int>();

        source.AsObservable().WindowTime(TimeSpan.FromMilliseconds(30)).Subscribe(window =>
        {
            var values = new List<int>();
            lock (windows)
            {
                windows.Add(values);
                if (windows.Count >= 2)
                {
                    signal.Set();
                }
            }

            window.Subscribe(values.Add);
        });

        source.OnNext(1);
        source.OnNext(2);

        Assert.That(signal.Wait(TimeSpan.FromSeconds(2)), Is.True, "a second window should open once the first one's time span elapses");
        source.OnCompleted();

        lock (windows)
        {
            Assert.That(windows[0], Is.EqualTo(new[] { 1, 2 }));
        }
    }

    [Test]
    public void ShouldCloseAWindowEarlyOnceMaxWindowSizeIsReached()
    {
        var windows = new List<List<int>>();
        var source = new RxSharp.Subjects.Subject<int>();

        source.AsObservable().WindowTime(TimeSpan.FromSeconds(5), maxWindowSize: 2).Subscribe(window =>
        {
            var values = new List<int>();
            windows.Add(values);
            window.Subscribe(values.Add);
        });

        source.OnNext(1);
        source.OnNext(2);
        source.OnNext(3);

        Assert.That(windows, Has.Count.EqualTo(2), "the first window should close (and a second one open) as soon as it reaches maxWindowSize, without waiting for the 5s time span");
        Assert.That(windows[0], Is.EqualTo(new[] { 1, 2 }));
        Assert.That(windows[1], Is.EqualTo(new[] { 3 }));
    }

    [Test]
    public void ShouldReturnASingleEmptyWindowIfSourceIsEmpty()
    {
        var windows = new List<List<int>>();
        var completed = false;

        Observable.Empty<int>().WindowTime(TimeSpan.FromSeconds(5)).Subscribe(
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

        Observable.ThrowError<int>(() => error).WindowTime(TimeSpan.FromSeconds(5)).Subscribe(
            window => window.Subscribe(onError: err => windowError = err),
            onError: err => outerError = err);

        Assert.That(windowError, Is.SameAs(error));
        Assert.That(outerError, Is.SameAs(error));
    }

    [Test]
    public void ShouldSupportOverlappingWindowsViaACreationInterval()
    {
        using var signal = new ManualResetEventSlim();
        var opened = 0;
        var source = new RxSharp.Subjects.Subject<int>();

        source.AsObservable().WindowTime(TimeSpan.FromMilliseconds(60), TimeSpan.FromMilliseconds(20)).Subscribe(_ =>
        {
            if (Interlocked.Increment(ref opened) >= 3)
            {
                signal.Set();
            }
        });

        Assert.That(signal.Wait(TimeSpan.FromSeconds(2)), Is.True, "a shorter creation interval than the window span should open overlapping windows");
    }
}
