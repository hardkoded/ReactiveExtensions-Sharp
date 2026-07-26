using RxSharp.Operators;
using RxSharp.Subjects;

namespace RxSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/repeatWhen-spec.ts.
[TestFixture]
public class RepeatWhenTests
{
    private static Observable<int> SucceedTwice(List<string> log)
    {
        var attempt = 0;
        return Observable.Defer(() =>
        {
            attempt++;
            log.Add($"subscribe {attempt}");
            return attempt == 1 ? Observable.Of(1, 2) : Observable.Of(3, 4);
        });
    }

    [Test]
    public void ShouldRepeatWhenNotifiedViaTheReturnedNotifierOnCompletion()
    {
        var results = new List<int>();
        var completed = false;
        var repeated = false;

        Observable.Of(1, 2)
            .RepeatWhen<int, Unit>(completions => completions.Map(_ =>
            {
                if (repeated)
                {
                    throw new InvalidOperationException("done");
                }

                repeated = true;
                return Unit.Default;
            }))
            .Subscribe(results.Add, onError: _ => { }, onComplete: () => completed = true);

        Assert.That(results, Is.EqualTo(new[] { 1, 2, 1, 2 }));
        Assert.That(completed, Is.False);
    }

    [Test]
    public void ShouldCompleteWhenTheNotifierCompletes()
    {
        var results = new List<int>();
        var completed = false;

        Observable.Of(1, 2).RepeatWhen<int, Unit>(_ => Observable.Empty<Unit>()).Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.EqualTo(new[] { 1, 2 }));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldForwardErrorFromTheNotifierAsTheFinalError()
    {
        var results = new List<int>();
        var thrown = new InvalidOperationException("notifier boom");
        Exception? received = null;

        Observable.Of(1, 2).RepeatWhen<int, Unit>(_ => Observable.ThrowError<Unit>(() => thrown)).Subscribe(results.Add, onError: err => received = err);

        Assert.That(results, Is.EqualTo(new[] { 1, 2 }));
        Assert.That(received, Is.SameAs(thrown));
    }

    [Test]
    public void ShouldPropagateErrorThrownFromTheNotifierSelectorFunction()
    {
        var thrown = new InvalidOperationException("bad!");
        Exception? received = null;

        Observable.Of(1, 2).RepeatWhen<int, Unit>(_ => throw thrown).Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(thrown));
    }

    [Test]
    public void ShouldNotInterceptSourceErrorsAndForwardThemImmediately()
    {
        var error = new InvalidOperationException("source boom");
        var results = new List<int>();
        Exception? received = null;
        var notifierSubscribed = false;

        new Observable<int>(subscriber =>
        {
            subscriber.OnNext(1);
            subscriber.OnNext(2);
            subscriber.OnError(error);
        }).RepeatWhen<int, Unit>(completions =>
        {
            notifierSubscribed = true;
            return completions;
        }).Subscribe(results.Add, onError: err => received = err);

        Assert.That(results, Is.EqualTo(new[] { 1, 2 }));
        Assert.That(received, Is.SameAs(error));
        Assert.That(notifierSubscribed, Is.False, "repeatWhen never intercepts errors, only completions");
    }

    [Test]
    public void ShouldResubscribeUsingAHotNotifierDrivenManually()
    {
        var log = new List<string>();
        var results = new List<int>();
        var completed = false;
        var notifier = new Subject<Unit>();

        SucceedTwice(log).RepeatWhen<int, Unit>(_ => notifier.AsObservable()).Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(log, Is.EqualTo(new[] { "subscribe 1" }));
        Assert.That(results, Is.EqualTo(new[] { 1, 2 }));
        Assert.That(completed, Is.False);

        notifier.OnNext(Unit.Default);

        Assert.That(log, Is.EqualTo(new[] { "subscribe 1", "subscribe 2" }));
        Assert.That(results, Is.EqualTo(new[] { 1, 2, 3, 4 }));

        notifier.OnCompleted();

        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldAlwaysFinalizeBeforeStartingTheNextCycleEvenWhenSynchronous()
    {
        // Mirrors RetryWhenTests's equivalent test: the notifier throws once it's been driven enough times,
        // converting to a terminal error, rather than completing (via e.g. Take) mid-recursion -- the latter
        // would reenter the still-executing outer Subject.OnNext call for this same notifier, which is a
        // different, untested hazard this test isn't meant to explore.
        var results = new List<object>();
        var source = new Observable<int>(subscriber =>
        {
            subscriber.Add(() => results.Add("finalizer"));
            subscriber.OnNext(1);
            subscriber.OnNext(2);
            subscriber.OnCompleted();
        });

        Exception? received = null;
        source.RepeatWhen<int, bool>(completions => completions.Map((_, i) => i < 3 ? true : throw new InvalidOperationException("done")))
            .Subscribe(value => results.Add(value), onError: err => received = err);

        Assert.That(
            results,
            Is.EqualTo(new object[] { 1, 2, "finalizer", 1, 2, "finalizer", 1, 2, "finalizer", 1, 2, "finalizer" }));
        Assert.That(received, Is.Not.Null);
    }
}
