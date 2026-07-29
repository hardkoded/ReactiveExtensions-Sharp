using ReactiveExtensionsSharp.Operators;
using ReactiveExtensionsSharp.Subjects;

namespace ReactiveExtensionsSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/skipUntil-spec.ts.
[TestFixture]
public class SkipUntilTests
{
    [Test]
    public void ShouldSkipValuesUntilTheNotifierEmits()
    {
        var source = new Subject<int>();
        var notifier = new Subject<Unit>();
        var results = new List<int>();

        source.AsObservable().SkipUntil(notifier.AsObservable()).Subscribe(results.Add);

        source.OnNext(1);
        source.OnNext(2);
        notifier.OnNext(Unit.Default);
        source.OnNext(3);
        source.OnNext(4);

        Assert.That(results, Is.EqualTo(new[] { 3, 4 }));
    }

    [Test]
    public void ShouldIgnoreNotifierValuesAfterTheFirst()
    {
        var source = new Subject<int>();
        var notifier = new Subject<Unit>();
        var results = new List<int>();

        source.AsObservable().SkipUntil(notifier.AsObservable()).Subscribe(results.Add);

        notifier.OnNext(Unit.Default);
        notifier.OnNext(Unit.Default);
        source.OnNext(1);

        Assert.That(results, Is.EqualTo(new[] { 1 }));
    }

    [Test]
    public void ShouldForwardNothingIfTheNotifierNeverEmits()
    {
        var results = new List<int>();
        var completed = false;

        Observable.Of(1, 2, 3).SkipUntil(Observable.Never<Unit>()).Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.Empty);
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldForwardNothingIfTheNotifierCompletesWithoutEmitting()
    {
        var results = new List<int>();
        var completed = false;

        Observable.Of(1, 2, 3).SkipUntil(Observable.Empty<Unit>()).Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.Empty);
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldForwardTheNotifiersErrorRatherThanSilentlyIgnoringIt()
    {
        var error = new InvalidOperationException("notifier boom");
        var results = new List<int>();
        Exception? received = null;

        Observable.Of(1, 2, 3).SkipUntil(Observable.ThrowError<Unit>(() => error)).Subscribe(results.Add, onError: err => received = err);

        Assert.That(results, Is.Empty);
        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldForwardSourceErrorsUnaffected()
    {
        var error = new InvalidOperationException("source boom");
        var results = new List<int>();
        Exception? received = null;

        new Observable<int>(subscriber =>
        {
            subscriber.OnNext(1);
            subscriber.OnError(error);
        }).SkipUntil(Observable.Of(Unit.Default)).Subscribe(results.Add, onError: err => received = err);

        Assert.That(results, Is.EqualTo(new[] { 1 }));
        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldForwardEverythingWhenTheNotifierEmitsSynchronouslyBeforeAnySourceValue()
    {
        var results = new List<int>();
        Observable.Of(1, 2, 3).SkipUntil(Observable.Of(Unit.Default)).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 1, 2, 3 }));
    }
}
