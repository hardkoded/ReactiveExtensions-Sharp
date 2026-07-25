using RxSharp.Extras;

namespace RxSharp.Tests.PuppeteerScenarios;

/// <summary>
/// Modeled directly on how Puppeteer's <c>Locator</c> actions (click/fill/hover/wait) compose
/// retry + cancellation + timeout: <c>pipe(retry({delay}), raceWith(fromAbortSignal(...), timeout(...)))</c>.
/// No upstream rxjs spec equivalent exists for this — it's Puppeteer's own combinator, not rxjs's — so this
/// suite is hand-written per the project's "Puppeteer usage must show up in tests" rule. This is also M2's
/// stated exit criteria: prove retry+race+timeout work together before touching the real puppeteer-sharp repo.
/// </summary>
[TestFixture]
public class RetryAndRaceWithSignalAndTimerTests
{
    private static Observable<string> FakeClick(int failuresBeforeSuccess, List<int> attempts)
    {
        var attempt = 0;
        return Observable.Defer(() =>
        {
            attempt++;
            attempts.Add(attempt);
            return attempt <= failuresBeforeSuccess
                ? Observable.ThrowError<string>(() => new InvalidOperationException("element not attached to the DOM"))
                : Observable.Of("clicked");
        });
    }

    [Test]
    public void ClickShouldSucceedAfterRetryingUntilTheElementIsReady()
    {
        var attempts = new List<int>();
        var results = new List<string>();
        var completed = false;
        using var signal = new ManualResetEventSlim();

        FakeClick(3, attempts)
            .RetryAndRaceWithSignalAndTimer(TimeSpan.FromSeconds(5), CancellationToken.None)
            .Subscribe(results.Add, onComplete: () =>
            {
                completed = true;
                signal.Set();
            });

        Assert.That(signal.Wait(TimeSpan.FromSeconds(2)), Is.True);
        Assert.That(attempts, Is.EqualTo(new[] { 1, 2, 3, 4 }));
        Assert.That(results, Is.EqualTo(new[] { "clicked" }));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ClickShouldFailWithATimeoutErrorIfTheElementNeverBecomesReady()
    {
        var attempts = new List<int>();
        using var signal = new ManualResetEventSlim();
        Exception? received = null;

        FakeClick(int.MaxValue, attempts)
            .RetryAndRaceWithSignalAndTimer(TimeSpan.FromMilliseconds(60), causeFactory: null, retryDelay: TimeSpan.FromMilliseconds(10), CancellationToken.None)
            .Subscribe(onError: err =>
            {
                received = err;
                signal.Set();
            });

        Assert.That(signal.Wait(TimeSpan.FromSeconds(2)), Is.True);
        Assert.That(received, Is.InstanceOf<TimeoutException>());
        Assert.That(attempts.Count, Is.GreaterThan(1), "should have retried at least once before timing out");
    }

    [Test]
    public void ClickShouldFailWithCancellationIfTheCallerAborts()
    {
        var attempts = new List<int>();
        using var cts = new CancellationTokenSource();
        using var signal = new ManualResetEventSlim();
        Exception? received = null;

        FakeClick(int.MaxValue, attempts)
            .RetryAndRaceWithSignalAndTimer(TimeSpan.FromSeconds(5), causeFactory: null, retryDelay: TimeSpan.FromMilliseconds(10), cts.Token)
            .Subscribe(onError: err =>
            {
                received = err;
                signal.Set();
            });

        Thread.Sleep(30);
        cts.Cancel();

        Assert.That(signal.Wait(TimeSpan.FromSeconds(2)), Is.True);
        Assert.That(received, Is.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public void ClickShouldUseASharedCauseForBothCancellationAndTimeout()
    {
        var attempts = new List<int>();
        using var signal = new ManualResetEventSlim();
        Exception? received = null;
        var cause = new TimeoutException("waiting for selector timed out");

        FakeClick(int.MaxValue, attempts)
            .RetryAndRaceWithSignalAndTimer(TimeSpan.FromMilliseconds(30), () => cause, TimeSpan.FromMilliseconds(10), CancellationToken.None)
            .Subscribe(onError: err =>
            {
                received = err;
                signal.Set();
            });

        Assert.That(signal.Wait(TimeSpan.FromSeconds(2)), Is.True);
        Assert.That(received, Is.SameAs(cause));
    }
}
