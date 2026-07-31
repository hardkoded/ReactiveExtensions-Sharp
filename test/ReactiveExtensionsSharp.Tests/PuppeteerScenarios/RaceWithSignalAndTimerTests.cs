using ReactiveExtensionsSharp.Extras;

namespace ReactiveExtensionsSharp.Tests.PuppeteerScenarios;

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
    public async Task ShouldResolveWithTheSourceValueIfItArrivesBeforeTimeoutOrCancellation()
    {
        var result = await Observable.Of("target-created")
            .RaceWithSignalAndTimer(TimeSpan.FromSeconds(5), CancellationToken.None)
            .ConfigureAwait(false);

        Assert.That(result, Is.EqualTo("target-created"));
    }

    [Test]
    public void ShouldFailWithATimeoutErrorIfTheSourceNeverEmits()
    {
        Assert.ThrowsAsync<TimeoutException>(() =>
            Observable.Never<string>().RaceWithSignalAndTimer(TimeSpan.FromMilliseconds(30), CancellationToken.None));
    }

    [Test]
    public void ShouldFailWithCancellationIfTheCallerAborts()
    {
        using var cts = new CancellationTokenSource();
        var task = Observable.Never<string>().RaceWithSignalAndTimer(TimeSpan.FromSeconds(5), cts.Token);

        cts.Cancel();

        Assert.ThrowsAsync<OperationCanceledException>(() => task);
    }

    [Test]
    public void ShouldUseASharedCauseForBothCancellationAndTimeout()
    {
        var cause = new TimeoutException("waiting for target timed out");

        var ex = Assert.ThrowsAsync<TimeoutException>(() =>
            Observable.Never<string>().RaceWithSignalAndTimer(TimeSpan.FromMilliseconds(30), () => cause, CancellationToken.None));

        Assert.That(ex, Is.SameAs(cause));
    }

    [Test]
    public async Task ShouldDisableTheTimeoutForAZeroOrNegativeValue()
    {
        var subject = new ReactiveExtensionsSharp.Subjects.Subject<string>();
        var task = subject.AsObservable().RaceWithSignalAndTimer(TimeSpan.Zero, CancellationToken.None);

        // Prove the timeout branch is truly disabled, not just long, by outliving what would otherwise fire.
        await Task.Delay(50).ConfigureAwait(false);
        subject.OnNext("late-target");

        var result = await task.ConfigureAwait(false);
        Assert.That(result, Is.EqualTo("late-target"));
    }

    [Test]
    public void ShouldFailWithASignalTasksExceptionIfItFiresBeforeTimeout()
    {
        var signalTcs = new TaskCompletionSource<bool>();
        var cause = new InvalidOperationException("session closed");
        signalTcs.SetException(cause);

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() =>
            Observable.Never<string>().RaceWithSignalAndTimer(TimeSpan.FromSeconds(5), causeFactory: null, signalTcs.Task));

        Assert.That(ex, Is.SameAs(cause));
    }

    [Test]
    public void ShouldFailWithATimeoutErrorIfItFiresBeforeAPendingSignalTask()
    {
        var signalTcs = new TaskCompletionSource<bool>();

        Assert.ThrowsAsync<TimeoutException>(() =>
            Observable.Never<string>().RaceWithSignalAndTimer(TimeSpan.FromMilliseconds(30), causeFactory: null, signalTcs.Task));
    }
}
