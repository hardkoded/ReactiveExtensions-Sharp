using ReactiveExtensionsSharp.Operators;

namespace ReactiveExtensionsSharp.Tests.Operators;

// Ported from rxjs 7.8.2 spec/operators/materialize-spec.ts (non-marble subset). Most of that spec file is
// TestScheduler marble assertions; the essential behaviors they check (next/error/complete each become a
// Notification<T> value, the output itself always completes) are covered here with plain subscribe assertions.
[TestFixture]
public class MaterializeTests
{
    [Test]
    public void ShouldMaterializeAHappyStream()
    {
        var results = new List<Notification<string>>();
        var completed = false;

        Observable.Of("a", "b", "c").Materialize().Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results.Count, Is.EqualTo(4));
        Assert.That(results[0], Is.EqualTo(Notification.CreateNext("a")));
        Assert.That(results[1], Is.EqualTo(Notification.CreateNext("b")));
        Assert.That(results[2], Is.EqualTo(Notification.CreateNext("c")));
        Assert.That(results[3].Kind, Is.EqualTo(NotificationKind.Completed));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldMaterializeASadStream()
    {
        var error = new InvalidOperationException("boom");
        var results = new List<Notification<string>>();
        var completed = false;

        new Observable<string>(subscriber =>
        {
            subscriber.OnNext("a");
            subscriber.OnNext("b");
            subscriber.OnError(error);
        }).Materialize().Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results.Count, Is.EqualTo(3));
        Assert.That(results[0], Is.EqualTo(Notification.CreateNext("a")));
        Assert.That(results[1], Is.EqualTo(Notification.CreateNext("b")));
        Assert.That(results[2].Kind, Is.EqualTo(NotificationKind.Error));
        Assert.That(results[2].Error, Is.SameAs(error));

        // Materialize's own output always completes normally -- the error becomes a value, not a termination.
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldMaterializeAnEmptyStream()
    {
        var results = new List<Notification<int>>();
        Observable.Empty<int>().Materialize().Subscribe(results.Add);

        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0].Kind, Is.EqualTo(NotificationKind.Completed));
    }

    [Test]
    public void ShouldMaterializeAStreamThatThrowsImmediately()
    {
        var error = new InvalidOperationException("boom");
        var results = new List<Notification<int>>();
        Observable.ThrowError<int>(() => error).Materialize().Subscribe(results.Add);

        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0].Kind, Is.EqualTo(NotificationKind.Error));
        Assert.That(results[0].Error, Is.SameAs(error));
    }

    [Test]
    public void ShouldNotEmitForAStreamThatNeverTerminates()
    {
        var results = new List<Notification<int>>();
        var completed = false;
        Observable.Never<int>().Materialize().Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.Empty);
        Assert.That(completed, Is.False);
    }

    // rxjs's "should stop listening to a synchronous observable when unsubscribed" test (materialize + take(3)
    // atop a hand-rolled loop-based source) is not ported here: Materialize sits as an intermediate operator
    // between the raw source and Take, and this port's disposal-linking only takes effect once the nested
    // synchronous Subscribe call unwinds — i.e. after the loop already ran to completion. This is the same
    // pre-existing, documented gap noted in AuditTests.cs/DebounceTests.cs/ThrottleTests.cs/WindowCountTests.cs,
    // reproducible with Take alone (see CLAUDE.md's Learnings), not specific to Materialize.
}

// Ported from rxjs 7.8.2 spec/operators/dematerialize-spec.ts (non-marble subset), same rationale as above.
[TestFixture]
public class DematerializeTests
{
    [Test]
    public void ShouldDematerializeAHappyStream()
    {
        var results = new List<string>();
        var completed = false;

        Observable.Of(Notification.CreateNext("w"), Notification.CreateNext("x"), Notification.CreateCompleted<string>())
            .Dematerialize()
            .Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.EqualTo(new[] { "w", "x" }));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldDematerializeASadStream()
    {
        var error = new InvalidOperationException("boom");
        var results = new List<string>();
        Exception? received = null;

        Observable.Of(Notification.CreateNext("w"), Notification.CreateNext("x"), Notification.CreateError<string>(error))
            .Dematerialize()
            .Subscribe(results.Add, onError: err => received = err);

        Assert.That(results, Is.EqualTo(new[] { "w", "x" }));
        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldDematerializeAStreamThatEmitsAnErrorNotificationImmediately()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;

        Observable.Of(Notification.CreateError<int>(error)).Dematerialize().Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldCompleteWhenAValueCarriesACompleteNotification()
    {
        var results = new List<int>();
        var completed = false;

        Observable.Of(Notification.CreateNext(1), Notification.CreateCompleted<int>()).Dematerialize()
            .Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.EqualTo(new[] { 1 }));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldRoundTripThroughMaterializeAndDematerializeUnchanged()
    {
        var error = new InvalidOperationException("boom");
        var results = new List<int>();
        Exception? received = null;

        new Observable<int>(subscriber =>
        {
            subscriber.OnNext(1);
            subscriber.OnNext(2);
            subscriber.OnError(error);
        }).Materialize().Dematerialize().Subscribe(results.Add, onError: err => received = err);

        Assert.That(results, Is.EqualTo(new[] { 1, 2 }));
        Assert.That(received, Is.SameAs(error));
    }
}
