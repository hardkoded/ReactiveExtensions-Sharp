using RxSharp.Operators;
using RxSharp.Subjects;

namespace RxSharp.Tests.Operators;

// Ported from rxjs 7.8.2 spec/operators/shareReplay-spec.ts (non-marble cases only -- the vast majority of that
// file is TestScheduler marble assertions, which RxSharp can't run yet with no virtual-time scheduler). The
// multi-subscriber/replay marble cases are adapted here into an equivalent, non-marble form: a plain Subject<T>
// stands in for rxjs's "hot" marble source, driven manually instead of on a virtual timeline.
[TestFixture]
public class ShareReplayTests
{
    [Test]
    public void ShouldMirrorASimpleSourceObservable()
    {
        var results = new List<int>();
        var completed = false;

        Observable.Of(1, 2, 3, 4, 5).ShareReplay().Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.EqualTo(new[] { 1, 2, 3, 4, 5 }));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldDoNothingIfResultIsNotSubscribed()
    {
        var subscribed = false;
        var source = new Observable<int>(_ => subscribed = true);

        source.ShareReplay();

        Assert.That(subscribed, Is.False);
    }

    [Test]
    public void ShouldMulticastTheSameValuesToMultipleObservers_BufferSize1()
    {
        var source = new Subject<int>();
        var shared = source.AsObservable().ShareReplay(1);

        var obs1 = new List<int>();
        shared.Subscribe(obs1.Add);
        source.OnNext(1);
        source.OnNext(2);

        // Late subscriber: bufferSize 1 replays only the single most recent value (2), not 1.
        var obs2 = new List<int>();
        shared.Subscribe(obs2.Add);

        source.OnNext(3);
        source.OnNext(4);
        source.OnCompleted();

        Assert.That(obs1, Is.EqualTo(new[] { 1, 2, 3, 4 }));
        Assert.That(obs2, Is.EqualTo(new[] { 2, 3, 4 }));
    }

    [Test]
    public void ShouldMulticastTheSameValuesToMultipleObservers_BufferSize2()
    {
        var source = new Subject<int>();
        var shared = source.AsObservable().ShareReplay(2);

        var obs1 = new List<int>();
        shared.Subscribe(obs1.Add);
        source.OnNext(1);
        source.OnNext(2);

        // Buffer holds exactly [1, 2] so far -- replays both.
        var obs2 = new List<int>();
        shared.Subscribe(obs2.Add);

        source.OnNext(3);

        // Buffer now holds the last 2: [2, 3] -- value 1 has fallen out.
        var obs3 = new List<int>();
        shared.Subscribe(obs3.Add);

        source.OnNext(4);
        source.OnCompleted();

        Assert.That(obs1, Is.EqualTo(new[] { 1, 2, 3, 4 }));
        Assert.That(obs2, Is.EqualTo(new[] { 1, 2, 3, 4 }));
        Assert.That(obs3, Is.EqualTo(new[] { 2, 3, 4 }));
    }

    [Test]
    public void ShouldMulticastAnErrorFromTheSourceToMultipleObservers()
    {
        var error = new InvalidOperationException("boom");
        var source = new Subject<int>();
        var shared = source.AsObservable().ShareReplay(1);

        var obs1 = new List<int>();
        Exception? obs1Error = null;
        shared.Subscribe(obs1.Add, err => obs1Error = err);
        source.OnNext(1);

        var obs2 = new List<int>();
        Exception? obs2Error = null;
        shared.Subscribe(obs2.Add, err => obs2Error = err);

        source.OnError(error);

        Assert.That(obs1, Is.EqualTo(new[] { 1 }));
        Assert.That(obs2, Is.EqualTo(new[] { 1 }));
        Assert.That(obs1Error, Is.SameAs(error));
        Assert.That(obs2Error, Is.SameAs(error));
    }

    [Test]
    public void ShouldReplayResultsToSubsequentSubscriptionsIfSourceCompletes()
    {
        var source = new Subject<int>();
        var shared = source.AsObservable().ShareReplay(2);

        var obs1 = new List<int>();
        var obs1Completed = false;
        shared.Subscribe(obs1.Add, onComplete: () => obs1Completed = true);

        source.OnNext(1);
        source.OnNext(2);
        source.OnNext(3);
        source.OnCompleted();

        // A subscriber arriving well after the source completed still gets the buffered replay + complete --
        // this is shareReplay's whole reason to exist (share() would instead reconnect to a fresh, already-done
        // source and get nothing).
        var obs2 = new List<int>();
        var obs2Completed = false;
        shared.Subscribe(obs2.Add, onComplete: () => obs2Completed = true);

        Assert.That(obs1, Is.EqualTo(new[] { 1, 2, 3 }));
        Assert.That(obs1Completed, Is.True);
        Assert.That(obs2, Is.EqualTo(new[] { 2, 3 }));
        Assert.That(obs2Completed, Is.True);
    }

    [Test]
    public void ShouldCompletelyRestartForSubsequentSubscriptionsIfSourceErrors()
    {
        var subscriptionCount = 0;
        Subject<int>? currentSource = null;
        var source = new Observable<int>(subscriber =>
        {
            subscriptionCount++;
            currentSource = new Subject<int>();
            return currentSource.Subscribe(subscriber);
        });

        var shared = source.ShareReplay(2);

        var obs1 = new List<int>();
        Exception? obs1Error = null;
        shared.Subscribe(obs1.Add, err => obs1Error = err);

        Assert.That(subscriptionCount, Is.EqualTo(1));
        currentSource!.OnNext(1);
        currentSource.OnNext(2);
        var error = new InvalidOperationException("boom");
        currentSource.OnError(error);

        Assert.That(obs1, Is.EqualTo(new[] { 1, 2 }));
        Assert.That(obs1Error, Is.SameAs(error));

        // Unlike the "completes" case above, an error resets the connector -- a later subscriber gets a fresh
        // ReplaySubject (empty buffer) and causes a brand new subscription to the source.
        var obs2 = new List<int>();
        shared.Subscribe(obs2.Add);

        Assert.That(subscriptionCount, Is.EqualTo(2));
        currentSource!.OnNext(3);
        currentSource.OnNext(4);

        Assert.That(obs2, Is.EqualTo(new[] { 3, 4 }));
    }

    [Test]
    public void ShouldStopListeningToASynchronousObservableWhenUnsubscribed()
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

        source.ShareReplay().Take(3).Subscribe();

        Assert.That(sideEffects, Is.EqualTo(new[] { 0, 1, 2 }));
    }

    [Test]
    public async Task ShouldOnlySubscribeOnceEachWithMultipleSynchronousSubscriptionsAndUnsubscriptions()
    {
        // Regression test adapted from https://github.com/ReactiveX/rxjs/issues/6760: combineLatest subscribes
        // to the same shared source twice (once per array entry); shareReplay must not connect to the
        // underlying source more than once even though both subscriptions happen synchronously.
        var subscriptions = 0;
        var source = Observable.Defer(() =>
        {
            subscriptions++;
            return new BehaviorSubject<int>(1).AsObservable();
        }).ShareReplay(1);

        await Observable.CombineLatest(source, source).FirstValueFrom().ConfigureAwait(false);

        Assert.That(subscriptions, Is.EqualTo(1));
    }

    [Test]
    public void ApplyingShareReplayToTwoDifferentSourcesKeepsThemIndependent()
    {
        var results1 = new List<int>();
        var results2 = new List<int>();

        Observable.Of(1, 2, 3).ShareReplay().Subscribe(results1.Add);
        Observable.Of(4, 5, 6).ShareReplay().Subscribe(results2.Add);

        Assert.That(results1, Is.EqualTo(new[] { 1, 2, 3 }));
        Assert.That(results2, Is.EqualTo(new[] { 4, 5, 6 }));
    }
}
