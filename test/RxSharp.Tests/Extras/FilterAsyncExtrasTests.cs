using RxSharp.Extras;

namespace RxSharp.Tests.Extras;

[TestFixture]
public class FilterAsyncExtrasTests
{
    [Test]
    public async Task ShouldKeepOnlyValuesWhoseAsyncPredicateResolvesTrue()
    {
        using var signal = new ManualResetEventSlim();
        var results = new List<int>();

        Observable.Of(1, 2, 3, 4, 5)
            .FilterAsync(async x =>
            {
                await Task.Delay(1).ConfigureAwait(false);
                return x % 2 == 0;
            })
            .Subscribe(
                value =>
                {
                    lock (results)
                    {
                        results.Add(value);
                    }
                },
                onComplete: () => signal.Set());

        Assert.That(signal.Wait(TimeSpan.FromSeconds(2)), Is.True);

        // MergeMap runs the async predicates concurrently, so order across values isn't guaranteed.
        Assert.That(results, Is.EquivalentTo(new[] { 2, 4 }));
        await Task.CompletedTask.ConfigureAwait(false);
    }

    [Test]
    public void ShouldPropagateErrorsFromTheAsyncPredicate()
    {
        var error = new InvalidOperationException("boom");
        using var signal = new ManualResetEventSlim();
        Exception? received = null;

        Observable.Of(1).FilterAsync<int>(_ => throw error).Subscribe(onError: err =>
        {
            received = err;
            signal.Set();
        });

        Assert.That(signal.Wait(TimeSpan.FromSeconds(2)), Is.True);
        Assert.That(received, Is.SameAs(error));
    }
}
