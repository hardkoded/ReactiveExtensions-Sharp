using ReactiveExtensionsSharp.Operators;
using ReactiveExtensionsSharp.Subjects;

namespace ReactiveExtensionsSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/retryWhen-spec.ts.
[TestFixture]
public class RetryWhenTests
{
    private static Observable<int> FailOnceThenSucceed(List<string> log)
    {
        var attempt = 0;
        return Observable.Defer(() =>
        {
            attempt++;
            log.Add($"subscribe {attempt}");
            if (attempt == 1)
            {
                return Observable.Concat(Observable.Of(1, 2), Observable.ThrowError<int>(() => new InvalidOperationException("bad")));
            }

            return Observable.Of(3, 4);
        });
    }

    [Test]
    public void ShouldRetryWhenNotifiedViaTheReturnedNotifierOnThrownError()
    {
        var results = new List<int>();
        var retried = false;
        Exception? received = null;

        Observable.Of(1, 2, 3)
            .Map(n =>
            {
                if (n == 3)
                {
                    throw new InvalidOperationException("bad");
                }

                return n;
            })
            .RetryWhen<int, string>(errors => errors.Map(err =>
            {
                if (retried)
                {
                    throw new InvalidOperationException("done");
                }

                retried = true;
                return err.Message;
            }))
            .Subscribe(results.Add, onError: err => received = err);

        Assert.That(results, Is.EqualTo(new[] { 1, 2, 1, 2 }));
        Assert.That(received, Is.Not.Null);
        Assert.That(received!.Message, Is.EqualTo("done"));
    }

    [Test]
    public void ShouldRetryWhenNotifiedAndCompleteOnReturnedCompletion()
    {
        var results = new List<int>();
        var completed = false;
        var errored = false;

        Observable.Of(1, 2, 3)
            .Map(n =>
            {
                if (n == 3)
                {
                    throw new InvalidOperationException("bad");
                }

                return n;
            })
            .RetryWhen<int, Unit>(_ => Observable.Empty<Unit>())
            .Subscribe(results.Add, onError: _ => errored = true, onComplete: () => completed = true);

        Assert.That(results, Is.EqualTo(new[] { 1, 2 }));
        Assert.That(errored, Is.False);
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldApplyAnEmptyNotifierOnAnEmptySource()
    {
        var completed = false;
        Observable.Empty<int>().RetryWhen<int, Unit>(_ => Observable.Empty<Unit>()).Subscribe(onComplete: () => completed = true);

        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldPropagateErrorThrownFromTheNotifierSelectorFunction()
    {
        var thrown = new InvalidOperationException("bad!");
        Exception? received = null;

        Observable.ThrowError<int>(() => new InvalidOperationException("boom"))
            .RetryWhen<int, Unit>(_ => throw thrown)
            .Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(thrown));
    }

    [Test]
    public void ShouldReplaceErrorWithCompleteUsingAnEmptyNotifier()
    {
        var results = new List<int>();
        var completed = false;
        var errored = false;

        new Observable<int>(subscriber =>
        {
            subscriber.OnNext(1);
            subscriber.OnNext(2);
            subscriber.OnError(new InvalidOperationException("boom"));
        }).RetryWhen<int, Unit>(_ => Observable.Empty<Unit>())
          .Subscribe(results.Add, onError: _ => errored = true, onComplete: () => completed = true);

        Assert.That(results, Is.EqualTo(new[] { 1, 2 }));
        Assert.That(errored, Is.False);
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldHideErrorsUsingANeverNotifierOnASourceWithEventualError()
    {
        var results = new List<int>();
        var errored = false;
        var completed = false;

        new Observable<int>(subscriber =>
        {
            subscriber.OnNext(1);
            subscriber.OnNext(2);
            subscriber.OnError(new InvalidOperationException("boom"));
        }).RetryWhen<int, Unit>(_ => Observable.Never<Unit>())
          .Subscribe(results.Add, onError: _ => errored = true, onComplete: () => completed = true);

        Assert.That(results, Is.EqualTo(new[] { 1, 2 }));
        Assert.That(errored, Is.False);
        Assert.That(completed, Is.False);
    }

    [Test]
    public void ShouldResubscribeUsingAHotNotifierDrivenManually()
    {
        var log = new List<string>();
        var results = new List<int>();
        var completed = false;
        var notifier = new Subject<Unit>();

        FailOnceThenSucceed(log).RetryWhen<int, Unit>(_ => notifier.AsObservable()).Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(log, Is.EqualTo(new[] { "subscribe 1" }));
        Assert.That(results, Is.EqualTo(new[] { 1, 2 }));
        Assert.That(completed, Is.False);

        notifier.OnNext(Unit.Default);

        Assert.That(log, Is.EqualTo(new[] { "subscribe 1", "subscribe 2" }));
        Assert.That(results, Is.EqualTo(new[] { 1, 2, 3, 4 }));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldAlwaysFinalizeBeforeStartingTheNextCycleEvenWhenSynchronous()
    {
        // See the equivalent RepeatTests test for why the source registers its own teardown via
        // subscriber.Add(...) (this port's idiom) instead of rxjs's return-a-teardown-function style.
        var results = new List<object>();
        var source = new Observable<int>(subscriber =>
        {
            subscriber.Add(() => results.Add("finalizer"));
            subscriber.OnNext(1);
            subscriber.OnNext(2);
            subscriber.OnError(new InvalidOperationException("bad"));
        });

        Exception? received = null;
        source.RetryWhen<int, bool>(errors => errors.Map((err, i) => i < 3 ? true : throw err))
            .Subscribe(value => results.Add(value), onError: err => received = err);

        Assert.That(
            results,
            Is.EqualTo(new object[] { 1, 2, "finalizer", 1, 2, "finalizer", 1, 2, "finalizer", 1, 2, "finalizer" }));
        Assert.That(received, Is.Not.Null);
    }
}
