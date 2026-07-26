using RxSharp.Operators;
using RxSharp.Subjects;

namespace RxSharp.Tests.Operators;

// Ported from rxjs 7.8.2 spec/operators/share-spec.ts (non-marble cases only -- the vast majority of that file
// is TestScheduler marble assertions, which RxSharp can't run yet with no virtual-time scheduler). The
// multi-subscriber/multicast marble cases ("should share the same values to multiple observers", "should share
// an error from the source to multiple observers") are adapted here into an equivalent, non-marble form: a plain
// Subject<T> stands in for rxjs's "hot" marble source, driven manually instead of on a virtual timeline.
[TestFixture]
public class ShareTests
{
    [Test]
    public void ShouldMirrorASimpleSourceObservable()
    {
        var results = new List<int>();
        var completed = false;

        Observable.Of(1, 2, 3, 4, 5).Share().Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.EqualTo(new[] { 1, 2, 3, 4, 5 }));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldShareASingleSubscription()
    {
        var subscriptionCount = 0;
        var source = new Observable<int>(_ => subscriptionCount++);
        var shared = source.Share();

        Assert.That(subscriptionCount, Is.EqualTo(0));

        shared.Subscribe();
        shared.Subscribe();

        Assert.That(subscriptionCount, Is.EqualTo(1));
    }

    [Test]
    public void ShouldNotChangeTheOutputOfTheObservableWhenError()
    {
        var error = new InvalidOperationException("boom");
        var source = Observable.Concat(Observable.Of(1, 2, 3), Observable.ThrowError<int>(() => error));

        var results = new List<int>();
        Exception? received = null;
        source.Share().Subscribe(results.Add, err => received = err);

        Assert.That(results, Is.EqualTo(new[] { 1, 2, 3 }));
        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldShareTheSameValuesToMultipleObservers()
    {
        var source = new Subject<int>();
        var shared = source.AsObservable().Share();

        var obs1 = new List<int>();
        var obs1Completed = false;
        shared.Subscribe(obs1.Add, onComplete: () => obs1Completed = true);
        source.OnNext(1);

        var obs2 = new List<int>();
        var obs2Completed = false;
        shared.Subscribe(obs2.Add, onComplete: () => obs2Completed = true);
        source.OnNext(2);

        var obs3 = new List<int>();
        var obs3Completed = false;
        shared.Subscribe(obs3.Add, onComplete: () => obs3Completed = true);
        source.OnNext(3);
        source.OnCompleted();

        Assert.That(obs1, Is.EqualTo(new[] { 1, 2, 3 }));
        Assert.That(obs2, Is.EqualTo(new[] { 2, 3 }));
        Assert.That(obs3, Is.EqualTo(new[] { 3 }));
        Assert.That(obs1Completed, Is.True);
        Assert.That(obs2Completed, Is.True);
        Assert.That(obs3Completed, Is.True);
    }

    [Test]
    public void ShouldShareAnErrorFromTheSourceToMultipleObservers()
    {
        var error = new InvalidOperationException("boom");
        var source = new Subject<int>();
        var shared = source.AsObservable().Share();

        var obs1 = new List<int>();
        Exception? obs1Error = null;
        shared.Subscribe(obs1.Add, err => obs1Error = err);
        source.OnNext(1);

        var obs2 = new List<int>();
        Exception? obs2Error = null;
        shared.Subscribe(obs2.Add, err => obs2Error = err);
        source.OnNext(2);
        source.OnError(error);

        Assert.That(obs1, Is.EqualTo(new[] { 1, 2 }));
        Assert.That(obs2, Is.EqualTo(new[] { 2 }));
        Assert.That(obs1Error, Is.SameAs(error));
        Assert.That(obs2Error, Is.SameAs(error));
    }

    [Test]
    public void ShouldDisconnectWhenLastSubscriberUnsubscribesAndReconnectOnNextSubscribe()
    {
        var subscriptionCount = 0;
        var source = new Observable<int>(_ => subscriptionCount++);
        var shared = source.Share();

        var sub1 = shared.Subscribe();
        var sub2 = shared.Subscribe();
        Assert.That(subscriptionCount, Is.EqualTo(1));

        sub1.Dispose();
        Assert.That(subscriptionCount, Is.EqualTo(1));

        sub2.Dispose();
        shared.Subscribe();

        Assert.That(subscriptionCount, Is.EqualTo(2));
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

        source.Share().Take(3).Subscribe();

        Assert.That(sideEffects, Is.EqualTo(new[] { 0, 1, 2 }));
    }

    [Test]
    public void ApplyingShareToTwoDifferentSourcesKeepsThemIndependent()
    {
        var results1 = new List<int>();
        var results2 = new List<int>();

        Observable.Of(1, 2, 3).Share().Subscribe(results1.Add);
        Observable.Of(4, 5, 6).Share().Subscribe(results2.Add);

        Assert.That(results1, Is.EqualTo(new[] { 1, 2, 3 }));
        Assert.That(results2, Is.EqualTo(new[] { 4, 5, 6 }));
    }

    // Ported/adapted from rxjs 7.8.2 spec/operators/share-spec.ts's "share(config)" describe block. rxjs also lets
    // resetOnError/resetOnComplete/resetOnRefCountZero be notifier-factory functions (deferred/conditional resets)
    // -- that variant is a deliberate scope cut for this port (see ShareConfig<T>'s remarks), so only the plain
    // bool cases are ported here.
    [Test]
    public void DefaultConfigBehavesTheSameAsTheParameterlessOverload()
    {
        var subscriptionCount = 0;
        var source = new Observable<int>(_ => subscriptionCount++);
        var shared = source.Share(new ShareConfig<int>());

        var sub1 = shared.Subscribe();
        var sub2 = shared.Subscribe();
        Assert.That(subscriptionCount, Is.EqualTo(1));

        sub1.Dispose();
        sub2.Dispose();
        shared.Subscribe();

        Assert.That(subscriptionCount, Is.EqualTo(2));
    }

    [Test]
    public void ShouldNotResetOnErrorIfConfiguredToDoSo()
    {
        var subscriptionCount = 0;
        var error = new InvalidOperationException("boom");
        var source = new Observable<int>(subscriber =>
        {
            subscriptionCount++;
            subscriber.OnNext(1);
            subscriber.OnError(error);
        });
        var shared = source.Share(new ShareConfig<int> { ResetOnError = false });

        var obs1 = new List<int>();
        Exception? obs1Error = null;
        shared.Subscribe(obs1.Add, err => obs1Error = err);

        Assert.That(subscriptionCount, Is.EqualTo(1));
        Assert.That(obs1, Is.EqualTo(new[] { 1 }));
        Assert.That(obs1Error, Is.SameAs(error));

        // The connector was kept (not reset), so a later subscriber attaches to the same, already-errored
        // connector and immediately receives the same error -- the source is never resubscribed.
        var obs2 = new List<int>();
        Exception? obs2Error = null;
        shared.Subscribe(obs2.Add, err => obs2Error = err);

        Assert.That(subscriptionCount, Is.EqualTo(1));
        Assert.That(obs2, Is.Empty);
        Assert.That(obs2Error, Is.SameAs(error));
    }

    [Test]
    public void ShouldNotResetOnCompleteIfConfiguredToDoSo()
    {
        var subscriptionCount = 0;
        var source = new Observable<int>(subscriber =>
        {
            subscriptionCount++;
            subscriber.OnNext(1);
            subscriber.OnCompleted();
        });
        var shared = source.Share(new ShareConfig<int> { ResetOnComplete = false });

        var obs1 = new List<int>();
        var obs1Completed = false;
        shared.Subscribe(obs1.Add, onComplete: () => obs1Completed = true);

        Assert.That(subscriptionCount, Is.EqualTo(1));
        Assert.That(obs1, Is.EqualTo(new[] { 1 }));
        Assert.That(obs1Completed, Is.True);

        // The connector was kept (not reset), so a later subscriber attaches to the same, already-completed
        // connector and immediately receives completion -- the source is never resubscribed.
        var obs2 = new List<int>();
        var obs2Completed = false;
        shared.Subscribe(obs2.Add, onComplete: () => obs2Completed = true);

        Assert.That(subscriptionCount, Is.EqualTo(1));
        Assert.That(obs2, Is.Empty);
        Assert.That(obs2Completed, Is.True);
    }

    [Test]
    public void ShouldNotResetOnRefCountZeroIfConfiguredToDoSo()
    {
        var subscriptionCount = 0;
        var source = new Observable<int>(_ => subscriptionCount++);
        var shared = source.Share(new ShareConfig<int> { ResetOnRefCountZero = false });

        var sub1 = shared.Subscribe();
        Assert.That(subscriptionCount, Is.EqualTo(1));

        // RefCount drops to zero here, but resetting on that is disabled -- the source subscription (and
        // connector) stays alive with nobody listening.
        sub1.Dispose();
        Assert.That(subscriptionCount, Is.EqualTo(1));

        shared.Subscribe();
        Assert.That(subscriptionCount, Is.EqualTo(1));
    }

    [Test]
    public void ShouldUseTheConnectorFunctionProvided()
    {
        var connectorCallCount = 0;
        var source = Observable.Of(1, 2, 3);
        var shared = source.Share(new ShareConfig<int>
        {
            Connector = () =>
            {
                connectorCallCount++;
                return new Subject<int>();
            },
        });

        var results = new List<int>();
        shared.Subscribe(results.Add);

        Assert.That(connectorCallCount, Is.EqualTo(1));
        Assert.That(results, Is.EqualTo(new[] { 1, 2, 3 }));
    }

    [Test]
    public void ACustomConnectorCanReplayValuesToLateSubscribersUnlikeThePlainSubjectDefault()
    {
        var source = new Subject<int>();

        // A ReplaySubject connector lets a late subscriber immediately see the most recent value -- distinct
        // from a plain Subject connector (the default), which only forwards values emitted after it subscribes.
        var shared = source.AsObservable().Share(new ShareConfig<int> { Connector = () => new ReplaySubject<int>(1) });

        var obs1 = new List<int>();
        shared.Subscribe(obs1.Add);
        source.OnNext(1);
        source.OnNext(2);

        var obs2 = new List<int>();
        shared.Subscribe(obs2.Add);

        Assert.That(obs1, Is.EqualTo(new[] { 1, 2 }));
        Assert.That(obs2, Is.EqualTo(new[] { 2 }));
    }
}
