using RxSharp.Subjects;

namespace RxSharp.Tests;

// Ported (non-marble subset) from rxjs 7.8.2 spec/observables/forkJoin-spec.ts.
[TestFixture]
public class ForkJoinTests
{
    [Test]
    public void ShouldEmitTheLastValueOfEachSourceOnceAllHaveCompleted()
    {
        var subjectA = new Subject<int>();
        var subjectB = new Subject<int>();
        var results = new List<IReadOnlyList<int>>();

        Observable.ForkJoin(subjectA.AsObservable(), subjectB.AsObservable()).Subscribe(results.Add);

        subjectA.OnNext(1);
        subjectA.OnNext(2);
        subjectA.OnCompleted();
        Assert.That(results, Is.Empty, "shouldn't emit until every source has completed");

        subjectB.OnNext(10);
        subjectB.OnCompleted();

        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0], Is.EqualTo(new[] { 2, 10 }));
    }

    [Test]
    public void ShouldAcceptASingleSource()
    {
        var results = new List<IReadOnlyList<int>>();
        var completed = false;

        Observable.ForkJoin(Observable.Of(1, 2, 3, 4)).Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0], Is.EqualTo(new[] { 4 }));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldCompleteWithoutEmittingIfAnySourceNeverEmitsAValue()
    {
        var results = new List<IReadOnlyList<int>>();
        var completed = false;
        Observable.ForkJoin(Observable.Of(1), Observable.Empty<int>()).Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.Empty);
        Assert.That(completed, Is.True);
    }

    // rxjs's "should complete early if any of source is empty and completes before than others": unlike
    // CombineLatest, ForkJoin completes as soon as ANY source completes without a value, even if other sources
    // are still active -- it does not wait for them.
    [Test]
    public void ShouldCompleteEarlyAsSoonAsAnySourceCompletesWithoutAValueEvenIfOthersAreStillActive()
    {
        var results = new List<IReadOnlyList<int>>();
        var completed = false;
        var neverEnding = new Subject<int>();

        Observable.ForkJoin(neverEnding.AsObservable(), Observable.Empty<int>()).Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(completed, Is.True, "the empty source completed with no value, so ForkJoin should complete early");
        Assert.That(results, Is.Empty);
    }

    [Test]
    public void ShouldCompleteWhenAllSourcesAreEmpty()
    {
        var completed = false;
        Observable.ForkJoin(Observable.Empty<int>(), Observable.Empty<int>()).Subscribe(onComplete: () => completed = true);

        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldNotCompleteWhenASourceNeverCompletes()
    {
        var completed = false;
        Observable.ForkJoin(Observable.Never<int>()).Subscribe(onComplete: () => completed = true);

        Assert.That(completed, Is.False);
    }

    [Test]
    public void ShouldNotCompleteWhenOneOfTheSourcesNeverCompletesEvenIfTheOtherHasValues()
    {
        var subjectB = new Subject<int>();
        var completed = false;

        Observable.ForkJoin(Observable.Never<int>(), subjectB.AsObservable()).Subscribe(onComplete: () => completed = true);

        subjectB.OnNext(1);
        subjectB.OnNext(2);
        subjectB.OnCompleted();

        Assert.That(completed, Is.False, "the other source never completes, so ForkJoin should keep waiting");
    }

    [Test]
    public void ShouldReturnEmptyWhenGivenNoSources()
    {
        var emitted = false;
        var completed = false;

        Observable.ForkJoin<int>().Subscribe(_ => emitted = true, onComplete: () => completed = true);

        Assert.That(emitted, Is.False);
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldPropagateAnErrorFromAnySourceImmediately()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;
        var neverEnding = new Subject<int>();

        Observable.ForkJoin(neverEnding.AsObservable(), Observable.ThrowError<int>(() => error)).Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldUnsubscribeTheOtherSourcesWhenOneOfThemErrors()
    {
        var otherTornDown = false;
        var other = new Observable<int>(subscriber => subscriber.Add(() => otherTornDown = true));
        var error = new InvalidOperationException("boom");

        Observable.ForkJoin(other, Observable.ThrowError<int>(() => error)).Subscribe(onError: _ => { });

        Assert.That(otherTornDown, Is.True);
    }

    [Test]
    public void ShouldUnsubscribeFromEverySourceWhenDownstreamUnsubscribes()
    {
        var aTornDown = false;
        var bTornDown = false;
        var a = new Observable<int>(subscriber => subscriber.Add(() => aTornDown = true));
        var b = new Observable<int>(subscriber => subscriber.Add(() => bTornDown = true));

        var subscription = Observable.ForkJoin(a, b).Subscribe();
        subscription.Dispose();

        Assert.That(aTornDown, Is.True);
        Assert.That(bTornDown, Is.True);
    }
}
