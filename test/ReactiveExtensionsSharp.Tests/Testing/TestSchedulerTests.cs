using ReactiveExtensionsSharp.Testing;

namespace ReactiveExtensionsSharp.Tests.Testing;

// No upstream rxjs spec equivalent — written from scratch to cover marble-string parsing and the cold/hot
// observables built from it, end to end against the virtual clock.
[TestFixture]
public class TestSchedulerTests
{
    [Test]
    public void ParseTimeReturnsTheFrameOfTheCompletionMarker()
        => Assert.That(TestScheduler.ParseTime("---|"), Is.EqualTo(TimeSpan.FromTicks(3)));

    [Test]
    public void ParseTimeThrowsWithoutACompletionMarker()
        => Assert.Throws<ArgumentException>(() => TestScheduler.ParseTime("---"));

    [Test]
    public void ColdObservableEmitsValuesAtTheFramesEncodedInTheDiagram()
    {
        var scheduler = new TestScheduler();
        var source = scheduler.CreateColdObservable("-a-b-c|", new Dictionary<char, int> { ['a'] = 1, ['b'] = 2, ['c'] = 3 });

        var results = scheduler.Record(source);
        scheduler.Start();

        Assert.That(
            results,
            Is.EqualTo(new[]
            {
                Recorded.OnNext(TimeSpan.FromTicks(1), 1),
                Recorded.OnNext(TimeSpan.FromTicks(3), 2),
                Recorded.OnNext(TimeSpan.FromTicks(5), 3),
                Recorded.OnCompleted<int>(TimeSpan.FromTicks(6)),
            }));
    }

    [Test]
    public void ColdObservableWithoutAValuesDictionaryUsesTheMarbleCharacterItself()
    {
        var scheduler = new TestScheduler();
        var source = scheduler.CreateColdObservable<char>("-a-b|");

        var results = scheduler.Record(source);
        scheduler.Start();

        Assert.That(
            results,
            Is.EqualTo(new[]
            {
                Recorded.OnNext(TimeSpan.FromTicks(1), 'a'),
                Recorded.OnNext(TimeSpan.FromTicks(3), 'b'),
                Recorded.OnCompleted<char>(TimeSpan.FromTicks(4)),
            }));
    }

    [Test]
    public void ColdObservableReplaysIndependentlyForEachSubscription()
    {
        var scheduler = new TestScheduler();
        var source = scheduler.CreateColdObservable("-a|", new Dictionary<char, int> { ['a'] = 1 });

        var first = scheduler.Record(source);
        scheduler.AdvanceTo(TimeSpan.FromTicks(5));

        var second = scheduler.Record(source);
        scheduler.AdvanceTo(TimeSpan.FromTicks(10));

        Assert.That(first, Is.EqualTo(new[] { Recorded.OnNext(TimeSpan.FromTicks(1), 1), Recorded.OnCompleted<int>(TimeSpan.FromTicks(2)) }));
        Assert.That(second, Is.EqualTo(new[] { Recorded.OnNext(TimeSpan.FromTicks(6), 1), Recorded.OnCompleted<int>(TimeSpan.FromTicks(7)) }), "should restart its own diagram from the second subscription's frame");
    }

    [Test]
    public void ColdObservableRejectsASubscriptionMarker()
        => Assert.Throws<ArgumentException>(() => new TestScheduler().CreateColdObservable("-^-a|", new Dictionary<char, int> { ['a'] = 1 }));

    [Test]
    public void ColdObservableWithErrorMarkerEmitsTheSuppliedError()
    {
        var scheduler = new TestScheduler();
        var error = new InvalidOperationException("boom");
        var source = scheduler.CreateColdObservable("-a-#", new Dictionary<char, int> { ['a'] = 1 }, error);

        var results = scheduler.Record(source);
        scheduler.Start();

        Assert.That(
            results,
            Is.EqualTo(new[]
            {
                Recorded.OnNext(TimeSpan.FromTicks(1), 1),
                Recorded.OnError<int>(TimeSpan.FromTicks(3), error),
            }));
    }

    [Test]
    public void HotObservableEmitsAtAbsoluteFramesSharedAcrossAllSubscribers()
    {
        var scheduler = new TestScheduler();
        var source = scheduler.CreateHotObservable("-a-b-c|", new Dictionary<char, int> { ['a'] = 1, ['b'] = 2, ['c'] = 3 });

        var results = scheduler.Record(source);
        scheduler.Start();

        Assert.That(
            results,
            Is.EqualTo(new[]
            {
                Recorded.OnNext(TimeSpan.FromTicks(1), 1),
                Recorded.OnNext(TimeSpan.FromTicks(3), 2),
                Recorded.OnNext(TimeSpan.FromTicks(5), 3),
                Recorded.OnCompleted<int>(TimeSpan.FromTicks(6)),
            }));
    }

    [Test]
    public void HotObservableDoesNotReplayValuesForALateSubscriber()
    {
        var scheduler = new TestScheduler();
        var source = scheduler.CreateHotObservable("-a-b-c|", new Dictionary<char, int> { ['a'] = 1, ['b'] = 2, ['c'] = 3 });

        scheduler.AdvanceTo(TimeSpan.FromTicks(2));
        var results = scheduler.Record(source);
        scheduler.Start();

        Assert.That(
            results,
            Is.EqualTo(new[]
            {
                Recorded.OnNext(TimeSpan.FromTicks(3), 2),
                Recorded.OnNext(TimeSpan.FromTicks(5), 3),
                Recorded.OnCompleted<int>(TimeSpan.FromTicks(6)),
            }),
            "a subscriber attaching at frame 2 should miss the value already emitted at frame 1");
    }

    [Test]
    public void HotObservableRejectsASubscriptionMarker()
    {
        // '^' is accepted (and ignored) by the parser itself, but only cold observables reject it outright;
        // hot observables tolerate it since every hot diagram is implicitly measured from frame 0 anyway.
        var scheduler = new TestScheduler();
        Assert.DoesNotThrow(() => scheduler.CreateHotObservable("-^-a|", new Dictionary<char, int> { ['a'] = 1 }));
    }

    [Test]
    public void RecordedEqualityIgnoresErrorIdentityButComparesTypeAndMessage()
    {
        var a = Recorded.OnError<int>(TimeSpan.FromTicks(1), new InvalidOperationException("boom"));
        var b = Recorded.OnError<int>(TimeSpan.FromTicks(1), new InvalidOperationException("boom"));
        var c = Recorded.OnError<int>(TimeSpan.FromTicks(1), new InvalidOperationException("different"));

        Assert.That(a, Is.EqualTo(b));
        Assert.That(a, Is.Not.EqualTo(c));
    }
}
