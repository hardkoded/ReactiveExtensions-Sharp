using RxSharp.Operators;
using RxSharp.Subjects;

namespace RxSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/pairwise-spec.ts.
[TestFixture]
public class PairwiseTests
{
    [Test]
    public void ShouldGroupConsecutiveEmissionsAsPairs()
    {
        var results = new List<(string, string)>();
        Observable.Of("a", "b", "c", "d", "e").Pairwise().Subscribe(results.Add);

        Assert.That(
            results,
            Is.EqualTo(new[] { ("a", "b"), ("b", "c"), ("c", "d"), ("d", "e") }));
    }

    [Test]
    public void ShouldNotEmitOnSingleElementStreams()
    {
        var results = new List<(int, int)>();
        var completed = false;
        Observable.Of(1).Pairwise().Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.Empty);
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldHandleMidStreamErrors()
    {
        var error = new InvalidOperationException("boom");
        var results = new List<(char, char)>();
        Exception? received = null;

        new Observable<char>(subscriber =>
        {
            subscriber.OnNext('b');
            subscriber.OnNext('c');
            subscriber.OnNext('d');
            subscriber.OnNext('e');
            subscriber.OnError(error);
        }).Pairwise().Subscribe(results.Add, onError: err => received = err);

        Assert.That(results, Is.EqualTo(new[] { ('b', 'c'), ('c', 'd'), ('d', 'e') }));
        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldHandleAnEmptySource()
    {
        var results = new List<(int, int)>();
        var completed = false;
        Observable.Empty<int>().Pairwise().Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.Empty);
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldHandleASourceThatImmediatelyThrows()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;
        Observable.ThrowError<int>(() => error).Pairwise().Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldBeRecursivelyReenterable()
    {
        var results = new List<(string, string)>();
        var subject = new Subject<string>();

        subject.AsObservable().Pairwise().Take(3).Subscribe(pair =>
        {
            results.Add(pair);
            subject.OnNext("c");
        });

        subject.OnNext("a");
        subject.OnNext("b");

        Assert.That(results, Is.EqualTo(new[] { ("a", "b"), ("b", "c"), ("c", "c") }));
    }

    // rxjs's "should stop listening to a synchronous observable when unsubscribed" test (pairwise + take(2)
    // atop a hand-rolled loop-based source) is not ported here: Pairwise sits as an intermediate operator
    // between the raw source and Take, and this port's disposal-linking only takes effect once the nested
    // synchronous Subscribe call unwinds — i.e. after the loop already ran to completion. This is the same
    // pre-existing, documented gap noted in AuditTests.cs/DebounceTests.cs/ThrottleTests.cs/WindowCountTests.cs,
    // reproducible with Take alone (see CLAUDE.md's Learnings), not specific to Pairwise.
}
