using RxSharp.Operators;
using RxSharp.Subjects;

namespace RxSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/distinctUntilChanged-spec.ts.
[TestFixture]
public class DistinctUntilChangedTests
{
    [Test]
    public void ShouldDistinguishBetweenValuesUsingDefaultEquality()
    {
        var results = new List<int>();
        Observable.Of(1, 1, 1, 2, 2, 2, 1, 1, 3, 3).DistinctUntilChanged().Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 1, 2, 1, 3 }));
    }

    [Test]
    public void ShouldSupportACustomComparer()
    {
        var results = new List<int>();
        Observable.Of(1, 2, 3, 4, 3, 5)
            .DistinctUntilChanged(Comparer(static (prevHigh, temp) => temp <= prevHigh))
            .Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 1, 2, 3, 4, 5 }));
    }

    [Test]
    public void ShouldSupportAKeySelector()
    {
        var results = new List<(string UpdatedBy, int Data)>();
        Observable.Of(
                ("blesh", 1),
                ("blesh", 2),
                ("ncjamieson", 3),
                ("ncjamieson", 4),
                ("blesh", 5))
            .DistinctUntilChanged(EqualityComparer<string>.Default, x => x.Item1)
            .Subscribe(results.Add);

        Assert.That(results.Select(x => x.UpdatedBy), Is.EqualTo(new[] { "blesh", "ncjamieson", "blesh" }));
    }

    [Test]
    public void ShouldPropagateErrorsFromTheKeySelector()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;
        Observable.Of(1, 2).DistinctUntilChanged<int, int>(EqualityComparer<int>.Default, _ => throw error).Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldPropagateErrorsWhenTheComparerThrows()
    {
        var results = new List<int>();
        Exception? received = null;

        Observable.Of(1, 2, 3, 4)
            .DistinctUntilChanged(Comparer((_, current) => current == 4 ? throw new InvalidOperationException("boom") : false))
            .Subscribe(results.Add, onError: err => received = err);

        Assert.That(results, Is.EqualTo(new[] { 1, 2, 3 }));
        Assert.That(received, Is.InstanceOf<InvalidOperationException>());
    }

    // Ported from rxjs's "should stop listening to a synchronous observable when unsubscribed" test. Already
    // covered generically by DisposalCascadeTests.ShouldCascadeDisposalThroughDistinctUntilChanged, but kept
    // here too as a direct, traceable port of the upstream spec case for this operator specifically.
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

        synchronousObservable.DistinctUntilChanged().Take(3).Subscribe(_ => { });

        Assert.That(sideEffects, Is.EqualTo(new[] { 0, 1, 2 }));
    }

    // Ported from rxjs's "should work properly with reentrant streams" test (see CLAUDE.md's Pairwise
    // reentrancy Learnings entry for why this matters). DistinctUntilChanged updates its `previousKey` state
    // BEFORE forwarding the value downstream (see DistinctUntilChanged.cs) -- the opposite order from Pairwise
    // -- which is the correct order here: the reentrant next(1) call must see the just-emitted value's key
    // already recorded as "previous", so it is correctly suppressed as a duplicate instead of re-emitted.
    [Test]
    public void ShouldWorkProperlyWithReentrantStreams()
    {
        var subject = new Subject<int>();
        var results = new List<int>();
        var count = 0;

        subject.AsObservable().DistinctUntilChanged().Subscribe(n =>
        {
            results.Add(n);

            if (++count > 2)
            {
                throw new InvalidOperationException("this should have only been hit once");
            }

            subject.OnNext(1);
        });

        subject.OnNext(1);

        Assert.That(results, Is.EqualTo(new[] { 1 }));
    }

    private static IEqualityComparer<int> Comparer(Func<int, int, bool> isSameGroup)
        => new FuncEqualityComparer(isSameGroup);

    private sealed class FuncEqualityComparer : IEqualityComparer<int>
    {
        private readonly Func<int, int, bool> _isSameGroup;

        public FuncEqualityComparer(Func<int, int, bool> isSameGroup) => _isSameGroup = isSameGroup;

        public bool Equals(int x, int y) => _isSameGroup(x, y);

        public int GetHashCode(int obj) => 0;
    }
}
