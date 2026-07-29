using ReactiveExtensionsSharp.Operators;

namespace ReactiveExtensionsSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/elementAt-spec.ts.
[TestFixture]
public class ElementAtTests
{
    [Test]
    public void ShouldReturnElementByZeroBasedIndex()
    {
        var results = new List<string>();
        Observable.Of("a", "b", "c", "d").ElementAt(2).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { "c" }));
    }

    [Test]
    public void ShouldReturnFirstElementByZeroBasedIndex()
    {
        var results = new List<string>();
        Observable.Of("a", "b", "c").ElementAt(0).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { "a" }));
    }

    [Test]
    public void ShouldAllowNullAsADefaultValue()
    {
        var received = "unset";
        Observable.Of("a", "a", "a").ElementAt(100, null!).Subscribe(value => received = value);

        Assert.That(received, Is.Null);
    }

    [Test]
    public void ShouldRaiseErrorIfIndexIsOutOfRangeAndNoDefaultValue()
    {
        Exception? received = null;
        Observable.Of("a").ElementAt(3).Subscribe(onError: err => received = err);

        Assert.That(received, Is.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void ShouldReturnDefaultValueIfIndexIsOutOfRange()
    {
        var results = new List<string>();
        var completed = false;
        Observable.Of("a").ElementAt(3, "42").Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.EqualTo(new[] { "42" }));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldRaiseErrorIfEmptyObservable()
    {
        Exception? received = null;
        Observable.Empty<int>().ElementAt(0).Subscribe(onError: err => received = err);

        Assert.That(received, Is.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void ShouldThrowIfIndexIsSmallerThanZero()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => Observable.Of(1, 2, 3).ElementAt(-1));
    }

    [Test]
    public void ShouldPropagateErrorFromTheSourceObservable()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;
        Observable.ThrowError<int>(() => error).ElementAt(0).Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldUnsubscribeFromTheSourceWhenTheIndexIsReached()
    {
        var subject = new ReactiveExtensionsSharp.Subjects.Subject<int>();
        var results = new List<int>();
        subject.AsObservable().ElementAt(1).Subscribe(results.Add);

        subject.OnNext(1);
        subject.OnNext(2);
        subject.OnNext(3);

        Assert.That(results, Is.EqualTo(new[] { 2 }));
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

        synchronousObservable.ElementAt(2).Subscribe(_ => { });

        Assert.That(sideEffects, Is.EqualTo(new[] { 0, 1, 2 }));
    }
}
