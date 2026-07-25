using RxSharp.Subjects;

namespace RxSharp.Tests.Subjects;

// Ported from rxjs 7.8.2 spec/subjects/ReplaySubject-spec.ts (non-marble cases).
[TestFixture]
public class ReplaySubjectTests
{
    [Test]
    public void ShouldReplayValuesUponSubscription()
    {
        var subject = new ReplaySubject<int>();
        subject.OnNext(1);
        subject.OnNext(2);
        subject.OnNext(3);

        var received = new List<int>();
        subject.Subscribe(received.Add);

        Assert.That(received, Is.EqualTo(new[] { 1, 2, 3 }));
    }

    [Test]
    public void ShouldReplayValuesAndComplete()
    {
        var subject = new ReplaySubject<int>();
        subject.OnNext(1);
        subject.OnNext(2);
        subject.OnNext(3);
        subject.OnCompleted();

        var received = new List<int>();
        var completed = false;
        subject.Subscribe(received.Add, onComplete: () => completed = true);

        Assert.That(received, Is.EqualTo(new[] { 1, 2, 3 }));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldReplayValuesAndError()
    {
        var subject = new ReplaySubject<int>();
        var error = new InvalidOperationException("fooey");
        subject.OnNext(1);
        subject.OnNext(2);
        subject.OnNext(3);
        subject.OnError(error);

        var received = new List<int>();
        Exception? receivedError = null;
        subject.Subscribe(received.Add, err => receivedError = err);

        Assert.That(received, Is.EqualTo(new[] { 1, 2, 3 }));
        Assert.That(receivedError, Is.SameAs(error));
    }

    [Test]
    public void ShouldOnlyReplayValuesWithinItsBufferSize()
    {
        var subject = new ReplaySubject<int>(2);
        subject.OnNext(1);
        subject.OnNext(2);
        subject.OnNext(3);

        var received = new List<int>();
        subject.Subscribe(received.Add);

        Assert.That(received, Is.EqualTo(new[] { 2, 3 }));
    }

    [Test]
    public void ShouldNotBufferNextedValuesAfterComplete()
    {
        var results = new List<object>();
        var subject = new ReplaySubject<int>();
        subject.OnNext(1);
        subject.OnNext(2);
        subject.OnCompleted();
        subject.OnNext(3);

        subject.Subscribe(value => results.Add(value), onComplete: () => results.Add("C"));

        Assert.That(results, Is.EqualTo(new object[] { 1, 2, "C" }));
    }

    [Test]
    public void ShouldNotBufferNextedValuesAfterError()
    {
        var results = new List<object>();
        var subject = new ReplaySubject<int>();
        subject.OnNext(1);
        subject.OnNext(2);
        subject.OnError(new InvalidOperationException("Boom!"));
        subject.OnNext(3);

        subject.Subscribe(value => results.Add(value), _ => results.Add("E"));

        Assert.That(results, Is.EqualTo(new object[] { 1, 2, "E" }));
    }

    [Test]
    public void ShouldOnlyReplayValuesWithinItsWindowTime()
    {
        var now = new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var subject = new ReplaySubject<int>(int.MaxValue, TimeSpan.FromSeconds(4), () => now);

        subject.OnNext(1);
        now += TimeSpan.FromSeconds(3);
        subject.OnNext(2);
        now += TimeSpan.FromSeconds(3);

        var received = new List<int>();
        subject.Subscribe(received.Add);

        Assert.That(received, Is.EqualTo(new[] { 2 }));
    }
}
