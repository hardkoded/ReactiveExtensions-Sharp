using RxSharp.Operators;

namespace RxSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/finalize-spec.ts.
[TestFixture]
public class FinalizeTests
{
    [Test]
    public void ShouldCallFinalizeAfterComplete()
    {
        var completed = false;
        var finalized = false;

        Observable.Of(1, 2, 3)
            .Finalize(() =>
            {
                Assert.That(completed, Is.True);
                finalized = true;
            })
            .Subscribe(onComplete: () => completed = true);

        Assert.That(finalized, Is.True);
    }

    [Test]
    public void ShouldCallFinalizeAfterError()
    {
        var thrown = false;
        var finalized = false;

        Observable.Of(1, 2, 3)
            .Map(x => x == 3 ? throw new InvalidOperationException("bad") : x)
            .Finalize(() =>
            {
                Assert.That(thrown, Is.True);
                finalized = true;
            })
            .Subscribe(onError: _ => thrown = true);

        Assert.That(finalized, Is.True);
    }

    [Test]
    public void ShouldCallFinalizeUponDisposal()
    {
        var finalized = false;
        var subscription = Observable.Never<int>().Finalize(() => finalized = true).Subscribe();

        Assert.That(finalized, Is.False);
        subscription.Dispose();

        Assert.That(finalized, Is.True);
    }

    [Test]
    public void ShouldCallTwoFinalizeInstancesInOrder()
    {
        var invoked = new List<int>();
        Observable.Of(1, 2, 3).Finalize(() => invoked.Add(1)).Finalize(() => invoked.Add(2)).Subscribe();

        Assert.That(invoked, Is.EqualTo(new[] { 1, 2 }));
    }

    [Test]
    public void ShouldHandleEmpty()
    {
        var executed = false;
        var completed = false;
        Observable.Empty<int>().Finalize(() => executed = true).Subscribe(onComplete: () => completed = true);

        Assert.That(executed, Is.True);
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldNotExecuteForANeverCompletingSource()
    {
        var executed = false;
        Observable.Never<int>().Finalize(() => executed = true).Subscribe();

        Assert.That(executed, Is.False);
    }

    [Test]
    public void ShouldHandleThrow()
    {
        var executed = false;
        Exception? received = null;
        Observable.ThrowError<int>(() => new InvalidOperationException("boom")).Finalize(() => executed = true).Subscribe(onError: err => received = err);

        Assert.That(executed, Is.True);
        Assert.That(received, Is.Not.Null);
    }

    [Test]
    public void ShouldFinalizeSourceBeforeSink()
    {
        var finalized = new List<string>();
        Observable.Of(42).Finalize(() => finalized.Add("source")).Finalize(() => finalized.Add("sink")).Subscribe();

        Assert.That(finalized, Is.EqualTo(new[] { "source", "sink" }));
    }

    [Test]
    public void ShouldFinalizeAfterTheSourcesOwnTeardown()
    {
        var order = new List<string>();
        var source = new Observable<int>(subscriber => subscriber.Add(() => order.Add("finalizer")));

        var subscription = source.Finalize(() => order.Add("finalize")).Subscribe();
        subscription.Dispose();

        Assert.That(order, Is.EqualTo(new[] { "finalizer", "finalize" }));
    }

    [Test]
    public void ShouldFinalizeAfterTheSourcesOwnTeardownWithSynchronousCompletion()
    {
        var order = new List<string>();
        var source = new Observable<int>(subscriber =>
        {
            subscriber.Add(() => order.Add("finalizer"));
            subscriber.OnCompleted();
        });

        source.Finalize(() => order.Add("finalize")).Subscribe();

        Assert.That(order, Is.EqualTo(new[] { "finalizer", "finalize" }));
    }

    // Regression test for the disposal-cascade fix (see CLAUDE.md Learnings): Finalize deliberately subscribes
    // the source with the exact downstream subscriber (no intermediate wrapper), so it never introduces the
    // gap in the first place -- see the "deliberately does not create any intermediate Subscriber" remark on
    // FinalizeOperator.Finalize. This confirms that holds for a fully-synchronous, self-checking source too.
    [Test]
    public void ShouldStopListeningToASynchronousObservableWhenUnsubscribed()
    {
        var sideEffects = new List<int>();
        var synchronousObservable = new Observable<int>(subscriber =>
        {
            for (var i = 0; !subscriber.IsDisposed && i < 10; i++)
            {
                sideEffects.Add(i);
                subscriber.OnNext(i);
            }
        });

        synchronousObservable.Finalize(() => { }).Take(3).Subscribe(_ => { });

        Assert.That(sideEffects, Is.EqualTo(new[] { 0, 1, 2 }));
    }
}
