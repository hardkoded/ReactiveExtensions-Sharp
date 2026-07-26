using RxSharp.Operators;
using RxSharp.Subjects;
using RxSharp.Testing;

namespace RxSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/retry-spec.ts.
[TestFixture]
public class RetryTests
{
    private static Observable<int> FailNTimesThenSucceed(int failuresBeforeSuccess, List<int> attemptLog)
    {
        var attempt = 0;
        return Observable.Defer(() =>
        {
            attempt++;
            attemptLog.Add(attempt);
            return attempt <= failuresBeforeSuccess
                ? Observable.ThrowError<int>(() => new InvalidOperationException($"attempt {attempt} failed"))
                : Observable.Of(42);
        });
    }

    [Test]
    public void ShouldResubscribeOnErrorUntilItSucceeds()
    {
        var attempts = new List<int>();
        var results = new List<int>();
        var completed = false;

        FailNTimesThenSucceed(2, attempts).Retry().Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(attempts, Is.EqualTo(new[] { 1, 2, 3 }));
        Assert.That(results, Is.EqualTo(new[] { 42 }));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldGiveUpAfterTheConfiguredCountAndPropagateTheLastError()
    {
        var attempts = new List<int>();
        Exception? received = null;

        FailNTimesThenSucceed(int.MaxValue, attempts).Retry(2).Subscribe(onError: err => received = err);

        Assert.That(attempts, Is.EqualTo(new[] { 1, 2, 3 }));
        Assert.That(received, Is.Not.Null);
        Assert.That(received!.Message, Is.EqualTo("attempt 3 failed"));
    }

    [Test]
    public void ShouldNotRetryAtAllWhenCountIsZero()
    {
        var attempts = new List<int>();
        Exception? received = null;

        FailNTimesThenSucceed(int.MaxValue, attempts).Retry(0).Subscribe(onError: err => received = err);

        Assert.That(attempts, Is.EqualTo(new[] { 1 }));
        Assert.That(received, Is.Not.Null);
    }

    [Test]
    public void ShouldPassThroughValuesEmittedBeforeAFailedAttempt()
    {
        var attempt = 0;
        var results = new List<int>();

        // Emits some values, then errors on the first attempt; succeeds on retry.
        var source = Observable.Defer(() =>
        {
            attempt++;
            if (attempt == 1)
            {
                return new Observable<int>(subscriber =>
                {
                    subscriber.OnNext(1);
                    subscriber.OnNext(2);
                    subscriber.OnError(new InvalidOperationException("boom"));
                });
            }

            return Observable.Of(3, 4);
        });

        source.Retry(1).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 1, 2, 3, 4 }));
    }

    [Test]
    public void ShouldWaitForTheDelayBeforeRetrying()
    {
        var attempts = new List<DateTimeOffset>();
        var source = Observable.Defer<int>(() =>
        {
            attempts.Add(DateTimeOffset.UtcNow);
            return attempts.Count == 1
                ? Observable.ThrowError<int>(() => new InvalidOperationException("boom"))
                : Observable.Of(1);
        });

        using var signal = new ManualResetEventSlim();
        source.Retry(1, TimeSpan.FromMilliseconds(50)).Subscribe(onComplete: () => signal.Set());

        Assert.That(signal.Wait(TimeSpan.FromSeconds(2)), Is.True);
        Assert.That(attempts, Has.Count.EqualTo(2));
        Assert.That((attempts[1] - attempts[0]).TotalMilliseconds, Is.GreaterThanOrEqualTo(40));
    }

    // Same scenario as above, but via TestScheduler: proves the retry attempt happens at exactly the given
    // delay — not merely "at least 40ms later" (the best a real-timer test can assert), but "at frame 50, not
    // one frame before".
    [Test]
    public void ShouldWaitForTheDelayBeforeRetrying_UsingVirtualTime()
    {
        var scheduler = new TestScheduler();
        var attempts = new List<TimeSpan>();
        var source = Observable.Defer<int>(() =>
        {
            attempts.Add(scheduler.Clock);
            return attempts.Count == 1
                ? Observable.ThrowError<int>(() => new InvalidOperationException("boom"))
                : Observable.Of(1);
        });

        var due = TimeSpan.FromTicks(50);
        var results = scheduler.Record(source.Retry(1, due, scheduler));

        Assert.That(attempts, Is.EqualTo(new[] { TimeSpan.Zero }), "the first attempt happens synchronously on subscribe");

        scheduler.Start();

        Assert.That(attempts, Is.EqualTo(new[] { TimeSpan.Zero, due }), "the retry should happen at exactly frame 50, not before and not after");
        Assert.That(results, Is.EqualTo(new[] { Recorded.OnNext(due, 1), Recorded.OnCompleted<int>(due) }));
    }

    // Regression test for the disposal-cascade fix (see CLAUDE.md Learnings): a fully-synchronous,
    // self-checking source composed with Retry and an early-completing Take must stop mid-loop, not just once
    // the whole synchronous call stack unwinds. This attempt never errors, so it exercises the "current attempt"
    // subscription being registered as a child of the downstream subscriber before Retry subscribes to it.
    [Test]
    public void ShouldCascadeDisposalThroughARetryAttemptThatNeverErrors()
    {
        var sideEffects = new List<int>();
        var source = new Observable<int>(subscriber =>
        {
            for (var i = 0; !subscriber.IsDisposed && i < 10; i++)
            {
                sideEffects.Add(i);
                subscriber.OnNext(i);
            }
        });

        source.Retry().Take(3).Subscribe(_ => { });

        Assert.That(sideEffects, Is.EqualTo(new[] { 0, 1, 2 }));
    }

    // Regression test proving Retry stops recursing into new attempts once downstream has unsubscribed, instead
    // of ignoring that fact and recursing forever for a rapidly, synchronously erroring source (a real
    // StackOverflowException risk without the fix). Also proves the per-attempt inner subscription is correctly
    // registered as a child of the downstream subscriber, since disposing that downstream subscriber is what
    // must stop the recursion.
    [Test]
    public void ShouldStopRetryingOnceDownstreamUnsubscribes()
    {
        var attempts = 0;

        // Built and assigned directly (rather than relying on the return value of `.Subscribe(...)`, which for
        // a fully-synchronous, immediately-erroring source with unlimited retries would never be assigned in
        // time — the very bug this whole fix is about) so it can be disposed synchronously, mid-recursion, from
        // inside the source itself.
        Subscriber<int> topSubscriber = null!;
        var source = new Observable<int>(subscriber =>
        {
            attempts++;
            if (attempts == 5)
            {
                topSubscriber.Dispose();
            }

            subscriber.OnError(new InvalidOperationException("boom"));
        });

        topSubscriber = Subscriber.Create<int>(onError: _ => { });
        source.Retry().Subscribe(topSubscriber);

        Assert.That(attempts, Is.EqualTo(5));
    }

    // attempt 3 emits a value and then errors again (rather than completing) so the reset -- or lack of it --
    // from that value has a chance to actually affect a LATER retry decision.
    private static Observable<int> AttemptsWithAResetPointThenSucceed(List<int> attemptLog)
    {
        var attempt = 0;
        return Observable.Defer<int>(() =>
        {
            attempt++;
            attemptLog.Add(attempt);
            return attempt switch
            {
                1 or 2 or 4 => Observable.ThrowError<int>(() => new InvalidOperationException($"boom {attempt}")),
                3 => Observable.Concat(Observable.Of(999), Observable.ThrowError<int>(() => new InvalidOperationException("boom 3"))),
                _ => Observable.Of(42),
            };
        });
    }

    [Test]
    public void ShouldResetTheRetryCounterOnceASuccessfulValueArrives()
    {
        var attemptLog = new List<int>();
        var results = new List<int>();
        var completed = false;

        AttemptsWithAResetPointThenSucceed(attemptLog).Retry(2, resetOnSuccess: true).Subscribe(results.Add, onComplete: () => completed = true);

        // Without resetOnSuccess this would exhaust its budget of 2 straight errors (attempts 1+2, then 3+4) and
        // never reach attempt 5 -- resetOnSuccess is what lets attempt 3's emitted value "forgive" attempts 1+2
        // so the count is only 1 (from attempt 4) by the time attempt 5 succeeds.
        Assert.That(results, Is.EqualTo(new[] { 999, 42 }));
        Assert.That(completed, Is.True);
        Assert.That(attemptLog, Is.EqualTo(new[] { 1, 2, 3, 4, 5 }));
    }

    [Test]
    public void ShouldNotResetTheRetryCounterWhenResetOnSuccessIsFalse()
    {
        var attemptLog = new List<int>();
        var results = new List<int>();
        Exception? received = null;

        AttemptsWithAResetPointThenSucceed(attemptLog).Retry(2, resetOnSuccess: false).Subscribe(results.Add, onError: err => received = err);

        // Attempts 1+2 exhaust the 2-retry budget outright; attempt 3's emitted value doesn't reset anything,
        // so its own subsequent error is the one that gets forwarded -- attempt 4/5 are never reached.
        Assert.That(results, Is.EqualTo(new[] { 999 }));
        Assert.That(received, Is.Not.Null);
        Assert.That(attemptLog, Is.EqualTo(new[] { 1, 2, 3 }));
    }

    [Test]
    public void ShouldRetryUsingANotifierSelectorDelay()
    {
        var results = new List<int>();
        var completed = false;
        var attempt = 0;
        var delaysRequested = new List<(Exception Error, int Attempt)>();
        var notifier = new Subject<Unit>();

        var source = Observable.Defer<int>(() =>
        {
            attempt++;
            return attempt == 1
                ? Observable.ThrowError<int>(() => new InvalidOperationException("boom"))
                : Observable.Of(1, 2);
        });

        source.Retry<int, Unit>((err, n) =>
        {
            delaysRequested.Add((err, n));
            return notifier.AsObservable();
        }).Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(delaysRequested, Has.Count.EqualTo(1));
        Assert.That(delaysRequested[0].Attempt, Is.EqualTo(1));
        Assert.That(results, Is.Empty, "should wait for the notifier before retrying");

        notifier.OnNext(Unit.Default);

        Assert.That(results, Is.EqualTo(new[] { 1, 2 }));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldForwardErrorFromTheDelaySelectorNotifier()
    {
        var notifierError = new InvalidOperationException("notifier boom");
        Exception? received = null;

        Observable.ThrowError<int>(() => new InvalidOperationException("boom"))
            .Retry<int, Unit>((_, _) => Observable.ThrowError<Unit>(() => notifierError))
            .Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(notifierError));
    }

    [Test]
    public void ShouldPropagateErrorThrownFromTheDelaySelectorFunctionItself()
    {
        var thrown = new InvalidOperationException("selector boom");
        Exception? received = null;

        Observable.ThrowError<int>(() => new InvalidOperationException("boom"))
            .Retry<int, Unit>((_, _) => throw thrown)
            .Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(thrown));
    }
}
