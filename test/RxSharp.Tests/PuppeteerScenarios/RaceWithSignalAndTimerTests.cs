using RxSharp.Extras;

namespace RxSharp.Tests.PuppeteerScenarios;

/// <summary>
/// Modeled on how Puppeteer's <c>Browser.waitForTarget</c>/<c>Page.waitForRequest</c>-shaped methods race a
/// single wait against cancellation and a timeout, without retrying: <c>pipe(filterAsync(predicate),
/// raceWith(fromAbortSignal(...), timeout(...)))</c>. Like <c>RetryAndRaceWithSignalAndTimer</c>, no upstream
/// rxjs spec equivalent exists for this combinator.
/// </summary>
[TestFixture]
public class RaceWithSignalAndTimerTests
{
    [Test]
    public void ShouldEmitTheSourceValueIfItArrivesBeforeTimeoutOrCancellation()
    {
        using var signal = new ManualResetEventSlim();
        var results = new List<string>();
        var completed = false;

        Observable.Of("target-created")
            .RaceWithSignalAndTimer(TimeSpan.FromSeconds(5), CancellationToken.None)
            .Subscribe(results.Add, onComplete: () =>
            {
                completed = true;
                signal.Set();
            });

        Assert.That(signal.Wait(TimeSpan.FromSeconds(2)), Is.True);
        Assert.That(results, Is.EqualTo(new[] { "target-created" }));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldFailWithATimeoutErrorIfTheSourceNeverEmits()
    {
        using var signal = new ManualResetEventSlim();
        Exception? received = null;

        Observable.Never<string>()
            .RaceWithSignalAndTimer(TimeSpan.FromMilliseconds(30), CancellationToken.None)
            .Subscribe(onError: err =>
            {
                received = err;
                signal.Set();
            });

        Assert.That(signal.Wait(TimeSpan.FromSeconds(2)), Is.True);
        Assert.That(received, Is.InstanceOf<TimeoutException>());
    }

    [Test]
    public void ShouldFailWithCancellationIfTheCallerAborts()
    {
        using var cts = new CancellationTokenSource();
        using var signal = new ManualResetEventSlim();
        Exception? received = null;

        Observable.Never<string>()
            .RaceWithSignalAndTimer(TimeSpan.FromSeconds(5), cts.Token)
            .Subscribe(onError: err =>
            {
                received = err;
                signal.Set();
            });

        cts.Cancel();

        Assert.That(signal.Wait(TimeSpan.FromSeconds(2)), Is.True);
        Assert.That(received, Is.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public void ShouldUseASharedCauseForBothCancellationAndTimeout()
    {
        using var signal = new ManualResetEventSlim();
        Exception? received = null;
        var cause = new TimeoutException("waiting for target timed out");

        Observable.Never<string>()
            .RaceWithSignalAndTimer(TimeSpan.FromMilliseconds(30), () => cause, CancellationToken.None)
            .Subscribe(onError: err =>
            {
                received = err;
                signal.Set();
            });

        Assert.That(signal.Wait(TimeSpan.FromSeconds(2)), Is.True);
        Assert.That(received, Is.SameAs(cause));
    }

    [Test]
    public void ShouldDisableTheTimeoutForAZeroOrNegativeValue()
    {
        using var signal = new ManualResetEventSlim();
        var results = new List<string>();

        var subject = new RxSharp.Subjects.Subject<string>();
        subject.AsObservable()
            .RaceWithSignalAndTimer(TimeSpan.Zero, CancellationToken.None)
            .Subscribe(results.Add, onComplete: signal.Set);

        // Prove the timeout branch is truly disabled, not just long, by outliving what would otherwise fire.
        Thread.Sleep(50);
        subject.OnNext("late-target");
        subject.OnCompleted();

        Assert.That(signal.Wait(TimeSpan.FromSeconds(2)), Is.True);
        Assert.That(results, Is.EqualTo(new[] { "late-target" }));
    }
}
