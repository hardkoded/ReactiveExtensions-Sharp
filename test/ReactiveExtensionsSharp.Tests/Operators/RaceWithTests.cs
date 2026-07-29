using ReactiveExtensionsSharp.Operators;
using ReactiveExtensionsSharp.Subjects;

namespace ReactiveExtensionsSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/observables/race-spec.ts, exercised via the raceWith
// pipeable operator rather than the standalone Race creation function -- RaceWith backs the Puppeteer-essential
// RetryAndRaceWithSignalAndTimer combinator (see CLAUDE.md), but previously had zero dedicated unit tests of
// its own, only indirect coverage via one PuppeteerScenarios integration test.
[TestFixture]
public class RaceWithTests
{
    [Test]
    public void ShouldMirrorWhicheverSourceEmitsFirst()
    {
        var slow = new Subject<int>();
        var fast = new Subject<int>();
        var results = new List<int>();

        slow.AsObservable().RaceWith(fast.AsObservable()).Subscribe(results.Add);

        fast.OnNext(1);
        slow.OnNext(2);
        fast.OnNext(3);

        Assert.That(results, Is.EqualTo(new[] { 1, 3 }));
    }

    [Test]
    public void ShouldUnsubscribeTheLosingSourcesAssoonAsOneWins()
    {
        var winner = new Subject<int>();
        var loserTornDown = false;
        var loser = new Observable<int>(subscriber => subscriber.Add(() => loserTornDown = true));

        winner.AsObservable().RaceWith(loser).Subscribe();

        Assert.That(loserTornDown, Is.False);
        winner.OnNext(1);

        Assert.That(loserTornDown, Is.True);
    }

    [Test]
    public void ShouldNeverForwardAValueFromALosingSourceEvenIfItEmitsLater()
    {
        var winner = new Subject<int>();
        var loser = new Subject<int>();
        var results = new List<int>();

        winner.AsObservable().RaceWith(loser.AsObservable()).Subscribe(results.Add);

        winner.OnNext(1);
        loser.OnNext(99);

        Assert.That(results, Is.EqualTo(new[] { 1 }));
    }

    [Test]
    public void ShouldWinTheRaceOnCompletionEvenWithoutAValue()
    {
        var winner = new Subject<int>();
        var loserSubscribed = false;
        var loser = new Observable<int>(subscriber =>
        {
            loserSubscribed = true;
            subscriber.Add(() => loserSubscribed = false);
        });
        var completed = false;

        winner.AsObservable().RaceWith(loser).Subscribe(onComplete: () => completed = true);

        winner.OnCompleted();

        Assert.That(completed, Is.True);
        Assert.That(loserSubscribed, Is.False);
    }

    [Test]
    public void ShouldWinTheRaceOnErrorEvenWithoutAValue()
    {
        var error = new InvalidOperationException("boom");
        var winner = new Subject<int>();
        var loser = new Subject<int>();
        Exception? received = null;

        winner.AsObservable().RaceWith(loser.AsObservable()).Subscribe(onError: err => received = err);

        winner.OnError(error);

        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldReturnSourceUnchangedWhenGivenNoOtherSources()
    {
        var results = new List<int>();
        Observable.Of(1, 2, 3).RaceWith().Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 1, 2, 3 }));
    }
}
