using ReactiveExtensionsSharp.Operators;

namespace ReactiveExtensionsSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/distinctUntilKeyChanged-spec.ts.
[TestFixture]
public class DistinctUntilKeyChangedTests
{
    [Test]
    public void ShouldDistinguishBetweenValuesByKey()
    {
        var results = new List<int>();
        Observable.Of(new Item(1), new Item(1), new Item(1), new Item(2), new Item(2), new Item(1))
            .DistinctUntilKeyChanged(x => x.Val)
            .Subscribe(x => results.Add(x.Val));

        Assert.That(results, Is.EqualTo(new[] { 1, 2, 1 }));
    }

    [Test]
    public void ShouldNotOmitIfSourceElementsAreAllDifferent()
    {
        var results = new List<int>();
        Observable.Of(new Item(1), new Item(2), new Item(3), new Item(4), new Item(5))
            .DistinctUntilKeyChanged(x => x.Val)
            .Subscribe(x => results.Add(x.Val));

        Assert.That(results, Is.EqualTo(new[] { 1, 2, 3, 4, 5 }));
    }

    [Test]
    public void ShouldEmitOnceIfComparerReturnsTrueAlways()
    {
        var results = new List<int>();
        Observable.Of(new Item(1), new Item(2), new Item(3), new Item(4), new Item(5))
            .DistinctUntilKeyChanged(x => x.Val, Comparer((_, _) => true))
            .Subscribe(x => results.Add(x.Val));

        Assert.That(results, Is.EqualTo(new[] { 1 }));
    }

    [Test]
    public void ShouldEmitAllIfComparerReturnsFalseAlways()
    {
        var results = new List<int>();
        Observable.Of(new Item(1), new Item(1), new Item(1))
            .DistinctUntilKeyChanged(x => x.Val, Comparer((_, _) => false))
            .Subscribe(x => results.Add(x.Val));

        Assert.That(results, Is.EqualTo(new[] { 1, 1, 1 }));
    }

    [Test]
    public void ShouldRaiseErrorIfSourceRaisesError()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;
        Observable.ThrowError<Item>(() => error).DistinctUntilKeyChanged(x => x.Val).Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldRaiseErrorWhenComparerThrows()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;
        Observable.Of(new Item(1), new Item(2), new Item(3))
            .DistinctUntilKeyChanged(x => x.Val, Comparer((_, y) =>
            {
                if (y == 3)
                {
                    throw error;
                }

                return false;
            }))
            .Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }

    private static IEqualityComparer<int> Comparer(Func<int, int, bool> isEqual) => new FuncEqualityComparer(isEqual);

    private sealed record Item(int Val);

    private sealed class FuncEqualityComparer : IEqualityComparer<int>
    {
        private readonly Func<int, int, bool> _isEqual;

        public FuncEqualityComparer(Func<int, int, bool> isEqual) => _isEqual = isEqual;

        public bool Equals(int x, int y) => _isEqual(x, y);

        public int GetHashCode(int obj) => 0;
    }
}
