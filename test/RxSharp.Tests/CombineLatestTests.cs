using RxSharp.Operators;
using RxSharp.Subjects;

namespace RxSharp.Tests;

// Ported (non-marble subset) from rxjs 7.8.2 spec/observables/combineLatest-spec.ts.
[TestFixture]
public class CombineLatestTests
{
    [Test]
    public void ShouldReturnEmptyWhenGivenNoSources()
    {
        var emitted = false;
        var completed = false;

        Observable.CombineLatest<int>().Subscribe(_ => emitted = true, onComplete: () => completed = true);

        Assert.That(emitted, Is.False);
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldNotEmitUntilEverySourceHasEmittedAtLeastOnce()
    {
        var subjectA = new Subject<int>();
        var subjectB = new Subject<string>();
        var results = new List<IReadOnlyList<object>>();

        Observable.CombineLatest<object>(subjectA.AsObservable().Map(x => (object)x), subjectB.AsObservable().Map(x => (object)x))
            .Subscribe(results.Add);

        subjectA.OnNext(1);
        Assert.That(results, Is.Empty, "B hasn't emitted yet");

        subjectB.OnNext("x");
        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0], Is.EqualTo(new object[] { 1, "x" }));

        subjectA.OnNext(2);
        Assert.That(results[1], Is.EqualTo(new object[] { 2, "x" }));
    }

    [Test]
    public void ShouldCompleteWhenAllSourcesCompleteWithoutEverEmitting()
    {
        var results = new List<IReadOnlyList<int>>();
        var completed = false;

        Observable.CombineLatest(Observable.Empty<int>(), Observable.Empty<int>()).Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.Empty);
        Assert.That(completed, Is.True);
    }

    // rxjs's "should work with empty and never": a source completing without a value must NOT eagerly complete
    // the whole combineLatest while another source is still active -- it must keep waiting on it (even though
    // the result can now never emit). This is the exact bug found and fixed while porting this test: the RxSharp
    // implementation used to short-circuit on `!hasValue[index]` the same way ForkJoin legitimately does, but
    // real rxjs's combineLatestInit has no such special case (see Observable.Creation.cs's CombineLatest remarks).
    [Test]
    public void ShouldNotCompleteEarlyWhenASourceCompletesWithoutAValueWhileAnotherIsStillActive()
    {
        var completed = false;
        var neverEnding = new Subject<int>();

        Observable.CombineLatest(Observable.Empty<int>(), neverEnding.AsObservable()).Subscribe(onComplete: () => completed = true);

        Assert.That(completed, Is.False, "must keep waiting on the still-active source");

        neverEnding.OnNext(1);
        Assert.That(completed, Is.False, "still shouldn't complete or emit -- the empty source can never contribute a value");

        neverEnding.OnCompleted();
        Assert.That(completed, Is.True, "now that every source has completed, the result should complete too");
    }

    // rxjs's "should work with hot-empty and hot-single": same scenario as above but with a synchronous (not
    // never-ending) second source, to make sure the eventual completion actually happens (not just "isn't early").
    [Test]
    public void ShouldCompleteOnceTheLastSourceCompletesEvenIfAnEarlierOneNeverEmitted()
    {
        var subjectB = new Subject<int>();
        var completed = false;

        Observable.CombineLatest(Observable.Empty<int>(), subjectB.AsObservable()).Subscribe(onComplete: () => completed = true);

        Assert.That(completed, Is.False);

        subjectB.OnNext(1);
        subjectB.OnCompleted();

        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldPropagateAnErrorFromAnySourceImmediately()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;
        var neverEnding = new Subject<int>();

        Observable.CombineLatest(neverEnding.AsObservable(), Observable.ThrowError<int>(() => error)).Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldNeverEmitOrCompleteWhenBothSourcesNeverEmitOrComplete()
    {
        var emitted = false;
        var completed = false;

        Observable.CombineLatest(Observable.Never<int>(), Observable.Never<int>()).Subscribe(_ => emitted = true, onComplete: () => completed = true);

        Assert.That(emitted, Is.False);
        Assert.That(completed, Is.False);
    }

    [Test]
    public void ShouldUnsubscribeFromEverySourceWhenDownstreamUnsubscribes()
    {
        var aTornDown = false;
        var bTornDown = false;
        var a = new Observable<int>(subscriber => subscriber.Add(() => aTornDown = true));
        var b = new Observable<int>(subscriber => subscriber.Add(() => bTornDown = true));

        var subscription = Observable.CombineLatest(a, b).Subscribe();
        subscription.Dispose();

        Assert.That(aTornDown, Is.True);
        Assert.That(bTornDown, Is.True);
    }
}
