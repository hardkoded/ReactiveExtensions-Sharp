using RxSharp.Subjects;

namespace RxSharp.Tests.Subjects;

// Ported from rxjs 7.8.2 spec/Subject-spec.ts (non-marble cases).
// Deliberate divergence: rxjs throws ObjectUnsubscribedError on next/error/complete after
// unsubscribe; RxSharp's Subject<T> silently ignores instead (matching Subscriber<T>'s own
// isStopped/IsDisposed guard semantics) rather than introducing a distinct exception type
// for it. See CLAUDE.md learnings.
[TestFixture]
public class SubjectTests
{
    [Test]
    public void ShouldPumpValuesToMultipleSubscribers()
    {
        var subject = new Subject<string>();
        var first = new List<string>();
        var second = new List<string>();
        var secondCompleted = false;

        subject.Subscribe(first.Add);
        subject.Subscribe(second.Add, onComplete: () => secondCompleted = true);

        subject.OnNext("foo");
        subject.OnNext("bar");
        subject.OnCompleted();

        Assert.That(first, Is.EqualTo(new[] { "foo", "bar" }));
        Assert.That(second, Is.EqualTo(new[] { "foo", "bar" }));
        Assert.That(secondCompleted, Is.True);
    }

    [Test]
    public void ShouldNotNextAfterCompleted()
    {
        var subject = new Subject<string>();
        var results = new List<string>();
        subject.Subscribe(x => results.Add(x), onComplete: () => results.Add("C"));

        subject.OnNext("a");
        subject.OnCompleted();
        subject.OnNext("b");

        Assert.That(results, Is.EqualTo(new[] { "a", "C" }));
    }

    [Test]
    public void ShouldNotNextAfterError()
    {
        var error = new InvalidOperationException("wut?");
        var subject = new Subject<string>();
        var results = new List<object>();
        subject.Subscribe(x => results.Add(x), err => results.Add(err));

        subject.OnNext("a");
        subject.OnError(error);
        subject.OnNext("b");

        Assert.That(results, Is.EqualTo(new object[] { "a", error }));
    }

    [Test]
    public void ShouldCleanOutUnsubscribedSubscribers()
    {
        var subject = new Subject<int>();
        var firstReceived = new List<int>();
        var secondReceived = new List<int>();

        var sub1 = subject.Subscribe(firstReceived.Add);
        var sub2 = subject.Subscribe(secondReceived.Add);

        subject.OnNext(1);
        sub1.Dispose();
        subject.OnNext(2);
        sub2.Dispose();
        subject.OnNext(3);

        Assert.That(firstReceived, Is.EqualTo(new[] { 1 }));
        Assert.That(secondReceived, Is.EqualTo(new[] { 1, 2 }));
    }

    [Test]
    public void LateSubscriberAfterCompleteShouldJustCompleteImmediately()
    {
        var subject = new Subject<int>();
        subject.OnCompleted();

        var completed = false;
        subject.Subscribe(onComplete: () => completed = true);

        Assert.That(completed, Is.True);
    }

    [Test]
    public void LateSubscriberAfterErrorShouldReceiveThatError()
    {
        var subject = new Subject<int>();
        var error = new InvalidOperationException("boom");
        subject.OnError(error);

        Exception? received = null;
        subject.Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void AsObservable_ShouldHideTheSubject()
    {
        var subject = new Subject<int>();
        var observable = subject.AsObservable();

        Assert.That(observable, Is.Not.InstanceOf<Subject<int>>());

        var values = new List<int>();
        observable.Subscribe(values.Add);
        subject.OnNext(1);

        Assert.That(values, Is.EqualTo(new[] { 1 }));
    }
}
