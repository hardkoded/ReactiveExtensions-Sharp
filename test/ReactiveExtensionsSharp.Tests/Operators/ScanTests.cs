using ReactiveExtensionsSharp.Operators;

namespace ReactiveExtensionsSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/scan-spec.ts.
[TestFixture]
public class ScanTests
{
    [Test]
    public void ShouldScanWithASeed()
    {
        var results = new List<int>();
        Observable.Of(1, 3, 5).Scan((acc, x) => acc + x, 0).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 1, 4, 9 }));
    }

    [Test]
    public void ShouldScanThingsIntoAnAccumulatingList()
    {
        var results = new List<List<string>>();
        Observable.Of("b", "c", "d").Scan((acc, x) => acc.Concat(new[] { x }).ToList(), new List<string>()).Subscribe(results.Add);

        Assert.That(results.Select(r => string.Join(",", r)), Is.EqualTo(new[] { "b", "b,c", "b,c,d" }));
    }

    [Test]
    public void ShouldProvideTheProperIndexIfSeedIsSkipped()
    {
        var seenIndices = new List<int>();
        Observable.Of(3, 3, 3).Scan((_, _, i) =>
        {
            seenIndices.Add(i);
            return 0;
        }).Subscribe();

        // The first value seeds the accumulator directly (no accumulator call), so the accumulator is first
        // called with index 1, on the second value, then index 2 on the third.
        Assert.That(seenIndices, Is.EqualTo(new[] { 1, 2 }));
    }

    [Test]
    public void ShouldScanWithoutASeedUsingTheFirstValueAsTheInitialAccumulator()
    {
        var results = new List<string>();
        Observable.Of("a", "b", "c", "d").Scan((acc, x) => acc + x).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { "a", "ab", "abc", "abcd" }));
    }

    [Test]
    public void ShouldPassCurrentIndexToTheSeededAccumulator()
    {
        var seenIndices = new List<int>();
        Observable.Of(1, 3, 5).Scan((acc, value, index) =>
        {
            seenIndices.Add(index);
            return acc + value;
        }, 0).Subscribe();

        Assert.That(seenIndices, Is.EqualTo(new[] { 0, 1, 2 }));
    }

    [Test]
    public void ShouldForwardErrorsFromTheSource()
    {
        var error = new InvalidOperationException("boom");
        var results = new List<int>();
        Exception? received = null;

        new Observable<int>(subscriber =>
        {
            subscriber.OnNext(1);
            subscriber.OnNext(2);
            subscriber.OnError(error);
        }).Scan((acc, x) => acc + x, 0).Subscribe(results.Add, onError: err => received = err);

        Assert.That(results, Is.EqualTo(new[] { 1, 3 }));
        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldForwardErrorsThrownByTheAccumulator()
    {
        var results = new List<List<string>>();
        Exception? received = null;

        Observable.Of("b", "c", "d").Scan(
            (acc, x) =>
            {
                if (x == "d")
                {
                    throw new InvalidOperationException("bad!");
                }

                var next = acc.Concat(new[] { x }).ToList();
                return next;
            },
            new List<string>()).Subscribe(results.Add, onError: err => received = err);

        Assert.That(results.Select(r => string.Join(",", r)), Is.EqualTo(new[] { "b", "b,c" }));
        Assert.That(received, Is.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void ShouldHandleAnEmptySource()
    {
        var results = new List<int>();
        var completed = false;
        Observable.Empty<int>().Scan((acc, x) => acc + x, 0).Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.Empty);
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldHandleANeverCompletingSource()
    {
        var completed = false;
        Observable.Never<int>().Scan((acc, x) => acc + x, 0).Subscribe(onComplete: () => completed = true);

        Assert.That(completed, Is.False);
    }

    [Test]
    public void ShouldHandleASourceThatImmediatelyThrows()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;
        Observable.ThrowError<int>(() => error).Scan((acc, x) => acc + x, 0).Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }

    // rxjs's "should stop listening to a synchronous observable when unsubscribed" test (scan + take(3) atop a
    // hand-rolled loop-based source) is not ported here: Scan sits as an intermediate operator between the raw
    // source and Take, and this port's disposal-linking only takes effect once the nested synchronous Subscribe
    // call unwinds — i.e. after the loop already ran to completion. This is the same pre-existing, documented
    // gap noted in AuditTests.cs/DebounceTests.cs/ThrottleTests.cs/WindowCountTests.cs, reproducible with Take
    // alone (see CLAUDE.md's Learnings), not specific to Scan.
}
