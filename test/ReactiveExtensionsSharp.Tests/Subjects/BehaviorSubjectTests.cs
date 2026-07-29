using ReactiveExtensionsSharp.Subjects;

namespace ReactiveExtensionsSharp.Tests.Subjects;

// Ported from rxjs 7.8.2 spec/subjects/BehaviorSubject-spec.ts (non-marble cases).
// Deliberate divergence: rxjs throws ObjectUnsubscribedError when getValue() is called after the subject has
// been unsubscribed; ReactiveExtensionsSharp has no such distinct exception type (see SubjectTests.cs), so a disposed
// BehaviorSubject<T> throws the ordinary ObjectDisposedException instead -- same pattern Subject<T> itself
// already uses for CheckDisposed().
[TestFixture]
public class BehaviorSubjectTests
{
    [Test]
    public void ShouldExtendSubject()
    {
        var subject = new BehaviorSubject<object?>(null);
        Assert.That(subject, Is.InstanceOf<Subject<object?>>());
    }

    [Test]
    public void ShouldThrowIfItHasReceivedAnErrorAndValueIsRead()
    {
        var subject = new BehaviorSubject<object?>(null);
        var error = new InvalidOperationException("derp");
        subject.OnError(error);

        Assert.That(() => subject.Value, Throws.Exception.SameAs(error));
    }

    [Test]
    public void ShouldThrowObjectDisposedExceptionIfValueIsReadAfterDispose()
    {
        var subject = new BehaviorSubject<string>("hi there");
        subject.Dispose();

        Assert.That(() => subject.Value, Throws.TypeOf<ObjectDisposedException>());
    }

    [Test]
    public void ShouldHaveAValuePropertyToRetrieveTheCurrentValue()
    {
        var subject = new BehaviorSubject<string>("staltz");
        Assert.That(subject.Value, Is.EqualTo("staltz"));

        subject.OnNext("oj");

        Assert.That(subject.Value, Is.EqualTo("oj"));
    }

    [Test]
    public void ShouldStartWithAnInitializationValue()
    {
        var subject = new BehaviorSubject<string>("foo");
        var received = new List<string>();
        var completed = false;

        subject.Subscribe(received.Add, onComplete: () => completed = true);

        subject.OnNext("bar");
        subject.OnCompleted();

        Assert.That(received, Is.EqualTo(new[] { "foo", "bar" }));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldPumpValuesToMultipleSubscribers()
    {
        var subject = new BehaviorSubject<string>("init");
        var first = new List<string>();
        var second = new List<string>();
        var secondCompleted = false;

        subject.Subscribe(first.Add);
        subject.Subscribe(second.Add, onComplete: () => secondCompleted = true);

        subject.OnNext("foo");
        subject.OnNext("bar");
        subject.OnCompleted();

        Assert.That(first, Is.EqualTo(new[] { "init", "foo", "bar" }));
        Assert.That(second, Is.EqualTo(new[] { "init", "foo", "bar" }));
        Assert.That(secondCompleted, Is.True);
    }

    [Test]
    public void ShouldNotPassValuesNextedAfterAComplete()
    {
        var subject = new BehaviorSubject<string>("init");
        var results = new List<string>();
        subject.Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { "init" }));

        subject.OnNext("foo");
        Assert.That(results, Is.EqualTo(new[] { "init", "foo" }));

        subject.OnCompleted();
        Assert.That(results, Is.EqualTo(new[] { "init", "foo" }));

        subject.OnNext("bar");
        Assert.That(results, Is.EqualTo(new[] { "init", "foo" }));
    }

    [Test]
    public void ShouldCleanOutUnsubscribedSubscribers()
    {
        var subject = new BehaviorSubject<string>("init");
        var firstReceived = new List<string>();
        var secondReceived = new List<string>();

        var sub1 = subject.Subscribe(firstReceived.Add);
        var sub2 = subject.Subscribe(secondReceived.Add);

        sub1.Dispose();
        subject.OnNext("next");
        sub2.Dispose();

        Assert.That(firstReceived, Is.EqualTo(new[] { "init" }));
        Assert.That(secondReceived, Is.EqualTo(new[] { "init", "next" }));
    }

    [Test]
    public void ShouldBeAnObserverWhichCanBeGivenToObservableSubscribe()
    {
        var source = Observable.Of(1, 2, 3, 4, 5);
        var subject = new BehaviorSubject<int>(0);
        var received = new List<int>();
        var completed = false;

        subject.Subscribe(received.Add, onComplete: () => completed = true);

        source.Subscribe(subject);

        Assert.That(received, Is.EqualTo(new[] { 0, 1, 2, 3, 4, 5 }));
        Assert.That(completed, Is.True);
        Assert.That(subject.Value, Is.EqualTo(5));
    }
}
