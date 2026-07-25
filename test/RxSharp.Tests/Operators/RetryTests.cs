using RxSharp.Operators;
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
}
