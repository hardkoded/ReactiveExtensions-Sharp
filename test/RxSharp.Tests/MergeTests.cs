using RxSharp.Operators;
using RxSharp.Subjects;

namespace RxSharp.Tests;

// Ported (non-marble subset) from rxjs 7.8.2 spec/observables/merge-spec.ts.
[TestFixture]
public class MergeTests
{
    [Test]
    public void ShouldMergeValuesFromAllSourcesAsTheyArrive()
    {
        var a = new Subject<int>();
        var b = new Subject<int>();
        var results = new List<int>();

        Observable.Merge(a.AsObservable(), b.AsObservable()).Subscribe(results.Add);

        a.OnNext(1);
        b.OnNext(10);
        a.OnNext(2);
        b.OnNext(20);

        Assert.That(results, Is.EqualTo(new[] { 1, 10, 2, 20 }));
    }

    [Test]
    public void ShouldCompleteOnceEverySourceHasCompleted()
    {
        var a = new Subject<int>();
        var b = new Subject<int>();
        var completed = false;

        Observable.Merge(a.AsObservable(), b.AsObservable()).Subscribe(onComplete: () => completed = true);

        a.OnCompleted();
        Assert.That(completed, Is.False);

        b.OnCompleted();
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldForwardAnErrorFromAnySourceImmediately()
    {
        var error = new InvalidOperationException("boom");
        var a = new Subject<int>();
        var b = new Subject<int>();
        Exception? received = null;

        Observable.Merge(a.AsObservable(), b.AsObservable()).Subscribe(onError: err => received = err);

        a.OnError(error);

        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldUnsubscribeTheRemainingSourcesWhenOneErrors()
    {
        var tornDown = false;
        var other = new Observable<int>(subscriber => subscriber.Add(() => tornDown = true));
        var error = new InvalidOperationException("boom");

        Observable.Merge(other, Observable.ThrowError<int>(() => error)).Subscribe(onError: _ => { });

        Assert.That(tornDown, Is.True);
    }

    [Test]
    public void ShouldCompleteImmediatelyWhenGivenNoSources()
    {
        var completed = false;
        Observable.Merge<int>().Subscribe(onComplete: () => completed = true);

        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldMergeASingleSourceUnchanged()
    {
        var results = new List<int>();
        Observable.Merge(Observable.Of(1, 2, 3)).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 1, 2, 3 }));
    }

    // Regression test for the disposal-cascade fix (see CLAUDE.md Learnings): found while writing this test,
    // Merge (like Concat) subscribed each source via the delegate-based Subscribe(onNext:...) overload assigned
    // into a list of disposables after Subscribe returned -- too late for a fully-synchronous source to observe
    // its own disposal mid-emission. Fixed via SubscribeChild, same as Concat.
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

        Observable.Merge(synchronousObservable).Take(3).Subscribe(_ => { });

        Assert.That(sideEffects, Is.EqualTo(new[] { 0, 1, 2 }));
    }
}
