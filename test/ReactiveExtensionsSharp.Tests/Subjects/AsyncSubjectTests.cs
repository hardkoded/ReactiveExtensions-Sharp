using ReactiveExtensionsSharp.Subjects;

namespace ReactiveExtensionsSharp.Tests.Subjects;

// Ported from rxjs 7.8.2 spec/subjects/AsyncSubject-spec.ts (the whole file -- none of it is marble-based).
[TestFixture]
public class AsyncSubjectTests
{
    [Test]
    public void ShouldEmitTheLastValueWhenComplete()
    {
        var subject = new AsyncSubject<int>();
        var results = new List<object>();
        subject.Subscribe(v => results.Add(v), onComplete: () => results.Add("done"));

        subject.OnNext(1);
        Assert.That(results, Is.Empty);
        subject.OnNext(2);
        Assert.That(results, Is.Empty);
        subject.OnCompleted();

        Assert.That(results, Is.EqualTo(new object[] { 2, "done" }));
    }

    [Test]
    public void ShouldEmitTheLastValueWhenSubscribingAfterComplete()
    {
        var subject = new AsyncSubject<int>();
        subject.OnNext(1);
        subject.OnNext(2);
        subject.OnCompleted();

        var results = new List<object>();
        subject.Subscribe(v => results.Add(v), onComplete: () => results.Add("done"));

        Assert.That(results, Is.EqualTo(new object[] { 2, "done" }));
    }

    [Test]
    public void ShouldKeepEmittingTheLastValueToSubsequentSubscriptions()
    {
        var subject = new AsyncSubject<int>();
        var results = new List<object>();
        var subscription = subject.Subscribe(v => results.Add(v), onComplete: () => results.Add("done"));

        subject.OnNext(1);
        Assert.That(results, Is.Empty);
        subject.OnNext(2);
        Assert.That(results, Is.Empty);
        subject.OnCompleted();
        Assert.That(results, Is.EqualTo(new object[] { 2, "done" }));

        subscription.Dispose();

        results = new List<object>();
        subject.Subscribe(v => results.Add(v), onComplete: () => results.Add("done"));
        Assert.That(results, Is.EqualTo(new object[] { 2, "done" }));
    }

    [Test]
    public void ShouldNotEmitValuesAfterComplete()
    {
        var subject = new AsyncSubject<int>();
        var results = new List<object>();
        subject.Subscribe(v => results.Add(v), onComplete: () => results.Add("done"));

        subject.OnNext(1);
        Assert.That(results, Is.Empty);
        subject.OnNext(2);
        Assert.That(results, Is.Empty);
        subject.OnCompleted();
        subject.OnNext(3);

        Assert.That(results, Is.EqualTo(new object[] { 2, "done" }));
    }

    [Test]
    public void ShouldNotAllowChangeValueAfterComplete()
    {
        var subject = new AsyncSubject<int>();
        var results = new List<object>();
        var otherResults = new List<object>();
        subject.Subscribe(v => results.Add(v), onComplete: () => results.Add("done"));

        subject.OnNext(1);
        Assert.That(results, Is.Empty);
        subject.OnCompleted();
        Assert.That(results, Is.EqualTo(new object[] { 1, "done" }));

        subject.OnNext(2);
        subject.Subscribe(v => otherResults.Add(v), onComplete: () => otherResults.Add("done"));

        Assert.That(otherResults, Is.EqualTo(new object[] { 1, "done" }));
    }

    [Test]
    public void ShouldNotEmitValuesIfUnsubscribedBeforeComplete()
    {
        var subject = new AsyncSubject<int>();
        var results = new List<object>();
        var subscription = subject.Subscribe(v => results.Add(v), onComplete: () => results.Add("done"));

        subject.OnNext(1);
        Assert.That(results, Is.Empty);
        subject.OnNext(2);
        Assert.That(results, Is.Empty);

        subscription.Dispose();

        subject.OnNext(3);
        Assert.That(results, Is.Empty);
        subject.OnCompleted();
        Assert.That(results, Is.Empty);
    }

    [Test]
    public void ShouldJustCompleteIfNoValueHasBeenNextedIntoIt()
    {
        var subject = new AsyncSubject<int>();
        var results = new List<object>();
        subject.Subscribe(v => results.Add(v), onComplete: () => results.Add("done"));

        Assert.That(results, Is.Empty);
        subject.OnCompleted();
        Assert.That(results, Is.EqualTo(new object[] { "done" }));
    }

    [Test]
    public void ShouldKeepEmittingCompleteToSubsequentSubscriptions()
    {
        var subject = new AsyncSubject<int>();
        var results = new List<object>();
        var subscription = subject.Subscribe(v => results.Add(v), onComplete: () => results.Add("done"));

        Assert.That(results, Is.Empty);
        subject.OnCompleted();
        Assert.That(results, Is.EqualTo(new object[] { "done" }));

        subscription.Dispose();
        results = new List<object>();

        subject.OnError(new InvalidOperationException(string.Empty));

        subject.Subscribe(v => results.Add(v), onComplete: () => results.Add("done"));
        Assert.That(results, Is.EqualTo(new object[] { "done" }));
    }

    [Test]
    public void ShouldOnlyErrorIfAnErrorIsPassedIntoIt()
    {
        var expected = new InvalidOperationException("bad");
        var subject = new AsyncSubject<int>();
        var results = new List<object>();
        subject.Subscribe(v => results.Add(v), err => results.Add(err));

        subject.OnNext(1);
        Assert.That(results, Is.Empty);

        subject.OnError(expected);
        Assert.That(results, Is.EqualTo(new object[] { expected }));
    }

    [Test]
    public void ShouldKeepEmittingErrorToSubsequentSubscriptions()
    {
        var expected = new InvalidOperationException("bad");
        var subject = new AsyncSubject<int>();
        var results = new List<object>();
        var subscription = subject.Subscribe(v => results.Add(v), err => results.Add(err));

        subject.OnNext(1);
        Assert.That(results, Is.Empty);

        subject.OnError(expected);
        Assert.That(results, Is.EqualTo(new object[] { expected }));

        subscription.Dispose();
        results = new List<object>();

        subject.Subscribe(v => results.Add(v), err => results.Add(err));
        Assert.That(results, Is.EqualTo(new object[] { expected }));
    }

    [Test]
    public void ShouldNotAllowSendCompleteAfterError()
    {
        var expected = new InvalidOperationException("bad");
        var subject = new AsyncSubject<int>();
        var results = new List<object>();
        var subscription = subject.Subscribe(v => results.Add(v), err => results.Add(err));

        subject.OnNext(1);
        Assert.That(results, Is.Empty);

        subject.OnError(expected);
        Assert.That(results, Is.EqualTo(new object[] { expected }));

        subscription.Dispose();
        results = new List<object>();

        subject.OnCompleted();
        subject.Subscribe(v => results.Add(v), err => results.Add(err));
        Assert.That(results, Is.EqualTo(new object[] { expected }));
    }

    [Test]
    public void ShouldNotBeReentrantViaComplete()
    {
        var subject = new AsyncSubject<int>();
        var calls = 0;
        subject.Subscribe(value =>
        {
            calls++;
            if (calls < 2)
            {
                subject.OnCompleted();
            }
        });

        subject.OnNext(1);
        subject.OnCompleted();

        Assert.That(calls, Is.EqualTo(1));
    }

    [Test]
    public void ShouldNotBeReentrantViaNext()
    {
        var subject = new AsyncSubject<int>();
        var calls = 0;
        subject.Subscribe(value =>
        {
            calls++;
            if (calls < 2)
            {
                subject.OnNext(value + 1);
            }
        });

        subject.OnNext(1);
        subject.OnCompleted();

        Assert.That(calls, Is.EqualTo(1));
    }

    [Test]
    public void ShouldAllowReentrantSubscriptions()
    {
        var subject = new AsyncSubject<int>();
        var results = new List<string>();

        subject.Subscribe(
            value =>
            {
                subject.Subscribe(
                    innerValue => results.Add("inner: " + (innerValue + innerValue)),
                    onComplete: () => results.Add("inner: done"));
                results.Add("outer: " + value);
            },
            onComplete: () => results.Add("outer: done"));

        subject.OnNext(1);
        Assert.That(results, Is.Empty);
        subject.OnCompleted();

        Assert.That(results, Is.EqualTo(new[] { "inner: 2", "inner: done", "outer: 1", "outer: done" }));
    }
}
