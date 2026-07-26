using RxSharp.Operators;

namespace RxSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/expand-spec.ts.
[TestFixture]
public class ExpandTests
{
    [Test]
    public void ShouldExpandRecursivelyUntilTheProjectedObservableCompletesWithoutEmitting()
    {
        var results = new List<int>();
        var completed = false;

        Observable.Of(1)
            .Expand(x => x >= 8 ? Observable.Empty<int>() : Observable.Of(x * 2))
            .Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.EqualTo(new[] { 1, 2, 4, 8 }));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldPassTheZeroBasedExpansionIndexToProject()
    {
        var indexes = new List<int>();
        Observable.Of(1)
            .Expand((_, i) =>
            {
                indexes.Add(i);
                return i < 2 ? Observable.Of(1) : Observable.Empty<int>();
            })
            .Subscribe();

        Assert.That(indexes, Is.EqualTo(new[] { 0, 1, 2 }));
    }

    [Test]
    public void ShouldForwardErrorThrownFromProject()
    {
        var thrown = new InvalidOperationException("boom");
        Exception? received = null;

        Observable.Of(1).Expand<int>(_ => throw thrown).Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(thrown));
    }

    [Test]
    public void ShouldForwardErrorFromAnInnerObservable()
    {
        var error = new InvalidOperationException("inner boom");
        var results = new List<int>();
        Exception? received = null;

        Observable.Of(1)
            .Expand(x => x == 1 ? Observable.ThrowError<int>(() => error) : Observable.Empty<int>())
            .Subscribe(results.Add, onError: err => received = err);

        Assert.That(results, Is.EqualTo(new[] { 1 }));
        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldHandleAnEmptySource()
    {
        var completed = false;
        Observable.Empty<int>().Expand(x => Observable.Of(x)).Subscribe(onComplete: () => completed = true);

        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldMergeMultipleInnerExpansionsConcurrently()
    {
        var results = new List<int>();
        var completed = false;

        Observable.Of(1, 10)
            .Expand(x => x is 1 or 10 ? Observable.Of(x + 1) : Observable.Empty<int>())
            .Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.EquivalentTo(new[] { 1, 10, 2, 11 }));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldStopRecursingOnceDownstreamHasUnsubscribed()
    {
        var results = new List<int>();
        Observable.Of(1).Expand(x => Observable.Of(x + 1)).Take(5).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 1, 2, 3, 4, 5 }));
    }
}
