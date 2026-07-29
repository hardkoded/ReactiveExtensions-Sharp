using ReactiveExtensionsSharp.Operators;

namespace ReactiveExtensionsSharp.Tests.Operators;

// Ported from rxjs 7.8.2 spec/operators/skip-spec.ts (non-marble cases; other marble cases converted to
// direct-value equivalents where timing is not the point being tested).
[TestFixture]
public class SkipTests
{
    [Test]
    public void ShouldSkipValuesBeforeATotal()
    {
        var results = new List<int>();
        Observable.Of(1, 2, 3, 4, 5).Skip(3).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 4, 5 }));
    }

    [Test]
    public void ShouldSkipAllValuesWithoutErrorIfTotalIsMoreThanActualNumberOfValues()
    {
        var results = new List<int>();
        var completed = false;
        Observable.Of(1, 2, 3, 4, 5).Skip(6).Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.Empty);
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldSkipAllValuesWithoutErrorIfTotalIsSameAsActualNumberOfValues()
    {
        var results = new List<int>();
        var completed = false;
        Observable.Of(1, 2, 3, 4, 5).Skip(5).Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.Empty);
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldNotSkipIfCountIsZero()
    {
        var results = new List<int>();
        Observable.Of(1, 2, 3, 4, 5).Skip(0).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 1, 2, 3, 4, 5 }));
    }

    [Test]
    public void ShouldNotSkipIfCountIsNegative()
    {
        var results = new List<int>();
        Observable.Of(1, 2, 3, 4, 5).Skip(-42).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 1, 2, 3, 4, 5 }));
    }

    [Test]
    public void ShouldRaiseErrorIfSkipCountIsMoreThanActualNumberOfEmitsAndSourceRaisesError()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;
        var results = new List<int>();
        Observable.Concat(Observable.Of(1, 2, 3, 4), Observable.ThrowError<int>(() => error))
            .Skip(6).Subscribe(results.Add, onError: err => received = err);

        Assert.That(results, Is.Empty);
        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldSkipValuesBeforeATotalAndRaiseErrorIfSourceRaisesError()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;
        var results = new List<int>();
        Observable.Concat(Observable.Of(1, 2, 3, 4), Observable.ThrowError<int>(() => error))
            .Skip(3).Subscribe(results.Add, onError: err => received = err);

        Assert.That(results, Is.EqualTo(new[] { 4 }));
        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldCompleteRegardlessOfSkipCountIfSourceIsEmpty()
    {
        var completed = false;
        Observable.Empty<int>().Skip(3).Subscribe(onComplete: () => completed = true);

        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldRaiseErrorIfSourceThrows()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;
        Observable.ThrowError<int>(() => error).Skip(3).Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
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

        synchronousObservable.Skip(1).Take(2).Subscribe(_ => { });

        Assert.That(sideEffects, Is.EqualTo(new[] { 0, 1, 2 }));
    }
}
