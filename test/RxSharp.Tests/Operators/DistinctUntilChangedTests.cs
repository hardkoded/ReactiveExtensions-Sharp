using RxSharp.Operators;

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
