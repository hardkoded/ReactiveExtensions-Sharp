using ReactiveExtensionsSharp.Operators;
using ReactiveExtensionsSharp.Subjects;

namespace ReactiveExtensionsSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/concatMap-spec.ts.
[TestFixture]
public class ConcatMapTests
{
    [Test]
    public void ShouldMapAndFlattenInEmissionOrder()
    {
        var results = new List<int>();
        Observable.Of(1, 2, 3).ConcatMap(x => Observable.Of(x, x * 10)).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 1, 10, 2, 20, 3, 30 }));
    }

    [Test]
    public void ShouldWaitForEachInnerToCompleteBeforeStartingTheNext()
    {
        var subjectA = new Subject<string>();
        var subjectB = new Subject<string>();
        var results = new List<string>();

        Observable.Of(subjectA.AsObservable(), subjectB.AsObservable()).ConcatMap(inner => inner).Subscribe(results.Add);

        subjectB.OnNext("from-b-too-early");
        Assert.That(results, Is.Empty, "B hasn't started yet — A hasn't completed.");

        subjectA.OnNext("a1");
        subjectA.OnCompleted();

        subjectB.OnNext("b1");
        subjectB.OnCompleted();

        Assert.That(results, Is.EqualTo(new[] { "a1", "b1" }));
    }

    [Test]
    public void ShouldPropagateErrorsFromAnInnerObservable()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;
        Observable.Of(1, 2).ConcatMap(_ => Observable.ThrowError<int>(() => error)).Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldCompleteImmediatelyForAnEmptySourceWithoutSubscribingToAnyInner()
    {
        var projectCalls = 0;
        var completed = false;
        Observable.Empty<int>().ConcatMap(x =>
        {
            projectCalls++;
            return Observable.Of(x);
        }).Subscribe(onComplete: () => completed = true);

        Assert.That(projectCalls, Is.EqualTo(0));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldNeverCompleteOrEmitForANeverSource()
    {
        var projectCalls = 0;
        var results = new List<int>();
        var completed = false;
        Observable.Never<int>().ConcatMap(x =>
        {
            projectCalls++;
            return Observable.Of(x);
        }).Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(projectCalls, Is.EqualTo(0));
        Assert.That(results, Is.Empty);
        Assert.That(completed, Is.False);
    }

    [Test]
    public void ShouldPropagateAnErrorFromAJustThrowSourceWithoutSubscribingToAnyInner()
    {
        var error = new InvalidOperationException("boom");
        var projectCalls = 0;
        Exception? received = null;
        Observable.ThrowError<int>(() => error).ConcatMap(x =>
        {
            projectCalls++;
            return Observable.Of(x);
        }).Subscribe(onError: err => received = err);

        Assert.That(projectCalls, Is.EqualTo(0));
        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldPropagateAnErrorFromTheProjectionFunction()
    {
        var error = new InvalidOperationException("boom");
        var results = new List<int>();
        Exception? received = null;

        Observable.Of(1, 2, 3).ConcatMap(x =>
        {
            if (x == 2)
            {
                throw error;
            }

            return Observable.Of(x);
        }).Subscribe(results.Add, onError: err => received = err);

        Assert.That(results, Is.EqualTo(new[] { 1 }));
        Assert.That(received, Is.SameAs(error));
    }

    // Ported from rxjs's "should finalize before moving to the next observable" test. Adapted to real async
    // timing (TaskPoolScheduler + a ManualResetEventSlim, per this project's established pattern for
    // time-based tests — see CLAUDE.md Learnings) instead of the TestScheduler's virtual time. Each inner
    // observable's teardown (analogous to rxjs's `finalize()`) must run before the next queued value's inner
    // observable is even created — matching the "always hold a live reference to your own inner subscriber"
    // discipline documented for Repeat/RetryWhen in CLAUDE.md.
    [Test]
    public void ShouldFinalizeEachInnerBeforeMovingToTheNextQueuedValue()
    {
        var results = new List<string>();
        var signal = new ManualResetEventSlim();

        Observable<string> Create(int n) => Observable.Defer(() =>
        {
            results.Add($"init {n}");
            return new Observable<string>(subscriber =>
            {
                TaskPoolScheduler.Instance.Schedule(
                    () =>
                    {
                        subscriber.OnNext($"next {n}");
                        subscriber.OnCompleted();
                    },
                    TimeSpan.FromMilliseconds(20));
                return new Subscription(() => results.Add($"finalized {n}"));
            });
        });

        Observable.Of(1, 2, 3).ConcatMap(Create).Subscribe(
            value => results.Add(value),
            onComplete: () => signal.Set());

        Assert.That(signal.Wait(TimeSpan.FromSeconds(2)), Is.True);
        Assert.That(
            results,
            Is.EqualTo(new[]
            {
                "init 1", "next 1", "finalized 1",
                "init 2", "next 2", "finalized 2",
                "init 3", "next 3", "finalized 3",
            }));
    }

    // Regression test for the disposal-cascade fix (see CLAUDE.md Learnings): a fully-synchronous, self-checking
    // source composed with ConcatMap through an early-completing downstream Take must stop mid-loop, not just
    // after the whole synchronous call stack unwinds. Mirrors DisposalCascadeTests.cs's pattern.
    [Test]
    public void ShouldCascadeDisposalToASynchronousSourceThroughTake()
    {
        var sideEffects = new List<int>();
        Observable<int> source = new(subscriber =>
        {
            for (var i = 0; !subscriber.IsDisposed && i < 10; i++)
            {
                sideEffects.Add(i);
                subscriber.OnNext(i);
            }
        });

        source.ConcatMap(x => Observable.Of(x)).Take(3).Subscribe(_ => { });

        Assert.That(sideEffects, Is.EqualTo(new[] { 0, 1, 2 }));
    }
}
