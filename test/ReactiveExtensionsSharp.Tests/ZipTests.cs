using ReactiveExtensionsSharp.Subjects;

namespace ReactiveExtensionsSharp.Tests;

// Ported (non-marble subset) from rxjs 7.8.2 spec/observables/zip-spec.ts.
[TestFixture]
public class ZipTests
{
    [Test]
    public void ShouldCombinePositionallyAndCompleteWhenTheShortestSourceIsExhausted()
    {
        var results = new List<IReadOnlyList<int>>();
        var completed = false;
        Observable.Zip(Observable.Of(1, 2, 3), Observable.Of(10, 20)).Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results.Select(r => r.ToArray()), Is.EqualTo(new[] { new[] { 1, 10 }, new[] { 2, 20 } }));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldReturnEmptyWhenGivenNoSources()
    {
        var emitted = false;
        var completed = false;

        Observable.Zip<int>().Subscribe(_ => emitted = true, onComplete: () => completed = true);

        Assert.That(emitted, Is.False);
        Assert.That(completed, Is.True);
    }

    // rxjs's "should end once one observable completes and its buffer is empty".
    [Test]
    public void ShouldEndOnceOneObservableCompletesAndItsBufferIsEmpty()
    {
        var subjectA = new Subject<string>();
        var subjectB = new Subject<string>();
        var subjectC = new Subject<string>(); // never completes
        var results = new List<IReadOnlyList<string>>();
        var completed = false;

        Observable.Zip(subjectA.AsObservable(), subjectB.AsObservable(), subjectC.AsObservable())
            .Subscribe(results.Add, onComplete: () => completed = true);

        subjectA.OnNext("a");
        subjectB.OnNext("d");
        subjectC.OnNext("h");
        Assert.That(results.Select(r => r.ToArray()), Is.EqualTo(new[] { new[] { "a", "d", "h" } }));

        subjectA.OnNext("b");
        subjectA.OnNext("c");
        subjectA.OnCompleted(); // a's buffer is now empty and it has completed
        Assert.That(completed, Is.False, "a still has buffered values to zip");

        subjectB.OnNext("e");
        subjectC.OnNext("i");
        Assert.That(results, Has.Count.EqualTo(2));
        Assert.That(completed, Is.False, "a's buffer is empty but there's still one more buffered value to drain");

        subjectB.OnNext("f");
        subjectC.OnNext("j");
        Assert.That(results, Has.Count.EqualTo(3));
        Assert.That(completed, Is.True, "a completed and its buffer is now empty, with nothing left to zip");
    }

    [Test]
    public void ShouldPropagateAnErrorFromAnySourceImmediately()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;
        var neverEnding = new Subject<int>();

        Observable.Zip(neverEnding.AsObservable(), Observable.ThrowError<int>(() => error)).Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }

    // rxjs's "should work with empty and error": once zip has already completed because a source completed with
    // an empty buffer, a later error from a different (still-subscribed) source must not surface -- zip has
    // already unsubscribed from it by then.
    [Test]
    public void ShouldNotPropagateAnErrorThatArrivesAfterZipAlreadyCompleted()
    {
        var errored = false;
        var completed = false;
        var subjectB = new Subject<int>();

        Observable.Zip(Observable.Empty<int>(), subjectB.AsObservable()).Subscribe(onError: _ => errored = true, onComplete: () => completed = true);

        Assert.That(completed, Is.True, "the empty source completed with an empty buffer, so zip should complete immediately");

        subjectB.OnError(new InvalidOperationException("too late"));

        Assert.That(errored, Is.False, "zip already completed and unsubscribed -- the error must not reach the subscriber");
    }

    [Test]
    public void ShouldNeverEmitOrCompleteWhenBothSourcesNeverEmitOrComplete()
    {
        var emitted = false;
        var completed = false;

        Observable.Zip(Observable.Never<int>(), Observable.Never<int>()).Subscribe(_ => emitted = true, onComplete: () => completed = true);

        Assert.That(emitted, Is.False);
        Assert.That(completed, Is.False);
    }

    [Test]
    public void ShouldCompleteImmediatelyWhenEitherSourceIsEmpty()
    {
        var emitted = false;
        var completed = false;

        Observable.Zip(Observable.Of(1, 2, 3), Observable.Empty<int>()).Subscribe(_ => emitted = true, onComplete: () => completed = true);

        Assert.That(emitted, Is.False);
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldStopAtTheShorterSourceWhenLengthsAreUneven()
    {
        var results = new List<IReadOnlyList<int>>();
        var completed = false;

        Observable.Zip(Observable.Of(1, 2), Observable.Of(10, 20, 30, 40)).Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results.Select(r => r.ToArray()), Is.EqualTo(new[] { new[] { 1, 10 }, new[] { 2, 20 } }));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldUnsubscribeFromEverySourceWhenDownstreamUnsubscribes()
    {
        var aTornDown = false;
        var bTornDown = false;
        var a = new Observable<int>(subscriber => subscriber.Add(() => aTornDown = true));
        var b = new Observable<int>(subscriber => subscriber.Add(() => bTornDown = true));

        var subscription = Observable.Zip(a, b).Subscribe();
        subscription.Dispose();

        Assert.That(aTornDown, Is.True);
        Assert.That(bTornDown, Is.True);
    }

    [Test]
    public void ShouldUnsubscribeFromTheRemainingSourcesOnceZipCompletes()
    {
        var bTornDown = false;
        var b = new Observable<int>(subscriber =>
        {
            subscriber.Add(() => bTornDown = true);
        });

        Observable.Zip(Observable.Empty<int>(), b).Subscribe();

        Assert.That(bTornDown, Is.True, "zip completed immediately (empty buffer on a) and should have unsubscribed from b");
    }
}
