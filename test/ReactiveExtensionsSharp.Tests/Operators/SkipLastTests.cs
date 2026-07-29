using ReactiveExtensionsSharp.Operators;

namespace ReactiveExtensionsSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/skipLast-spec.ts.
[TestFixture]
public class SkipLastTests
{
    [Test]
    public void ShouldSkipTheLastTwoValues()
    {
        var results = new List<int>();
        Observable.Of(1, 2, 3, 4, 5).SkipLast(2).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 1, 2, 3 }));
    }

    [Test]
    public void ShouldSkipASingleTrailingValue()
    {
        var results = new List<int>();
        Observable.Of(1, 2, 3).SkipLast(1).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 1, 2 }));
    }

    [Test]
    public void ShouldMirrorSourceUnchangedWhenCountIsZero()
    {
        var results = new List<int>();
        Observable.Of(1, 2, 3).SkipLast(0).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 1, 2, 3 }));
    }

    [Test]
    public void ShouldMirrorSourceUnchangedWhenCountIsNegative()
    {
        var results = new List<int>();
        Observable.Of(1, 2, 3).SkipLast(-1).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 1, 2, 3 }));
    }

    [Test]
    public void ShouldEmitNothingWhenSourceHasFewerValuesThanCount()
    {
        var results = new List<int>();
        var completed = false;
        Observable.Of(1, 2).SkipLast(5).Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.Empty);
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldForwardErrorsImmediatelyWithoutFlushingTheBuffer()
    {
        var error = new InvalidOperationException("boom");
        var results = new List<int>();
        Exception? received = null;

        new Observable<int>(subscriber =>
        {
            subscriber.OnNext(1);
            subscriber.OnNext(2);
            subscriber.OnError(error);
        }).SkipLast(2).Subscribe(results.Add, onError: err => received = err);

        Assert.That(results, Is.Empty);
        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldHandleAnEmptySource()
    {
        var completed = false;
        Observable.Empty<int>().SkipLast(2).Subscribe(onComplete: () => completed = true);

        Assert.That(completed, Is.True);
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

        synchronousObservable.SkipLast(1).Take(3).Subscribe(_ => { });

        Assert.That(sideEffects, Is.EqualTo(new[] { 0, 1, 2, 3 }));
    }
}
