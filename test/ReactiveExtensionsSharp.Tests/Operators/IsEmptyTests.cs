using ReactiveExtensionsSharp.Operators;

namespace ReactiveExtensionsSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/isEmpty-spec.ts.
[TestFixture]
public class IsEmptyTests
{
    [Test]
    public void ShouldReturnTrueIfSourceIsEmpty()
    {
        var results = new List<bool>();
        var completed = false;
        Observable.Empty<int>().IsEmpty().Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.EqualTo(new[] { true }));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldReturnFalseIfSourceEmitsElement()
    {
        var results = new List<bool>();
        var completed = false;
        Observable.Of("a", "b").IsEmpty().Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.EqualTo(new[] { false }));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldEmitFalseAsSoonAsTheFirstValueArrivesWithoutWaitingForCompletion()
    {
        var subject = new ReactiveExtensionsSharp.Subjects.Subject<int>();
        var results = new List<bool>();
        subject.AsObservable().IsEmpty().Subscribe(results.Add);

        subject.OnNext(1);
        subject.OnNext(2);

        Assert.That(results, Is.EqualTo(new[] { false }));
    }

    [Test]
    public void ShouldRaiseErrorIfSourceRaisesError()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;
        Observable.ThrowError<int>(() => error).IsEmpty().Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldNotCompleteIfSourceNeverEmits()
    {
        var subject = new ReactiveExtensionsSharp.Subjects.Subject<int>();
        var results = new List<bool>();
        var completed = false;
        subject.AsObservable().IsEmpty().Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.Empty);
        Assert.That(completed, Is.False);
    }

    [Test]
    public void ShouldStopListeningToASynchronousObservableWhenUnsubscribed()
    {
        var sideEffects = new List<int>();
        var synchronousObservable = new Observable<int>(subscriber =>
        {
            for (var i = 0; !subscriber.IsDisposed && i < 10; i++)
            {
                sideEffects.Add(i);
                subscriber.OnNext(i);
            }
        });

        synchronousObservable.IsEmpty().Subscribe(_ => { });

        Assert.That(sideEffects, Is.EqualTo(new[] { 0 }));
    }
}
