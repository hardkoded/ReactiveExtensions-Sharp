using RxSharp.Operators;

namespace RxSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/endWith-spec.ts.
[TestFixture]
public class EndWithTests
{
    [Test]
    public void ShouldAppendASingleValueAfterTheSourceCompletes()
    {
        var results = new List<int>();
        Observable.Of(1, 2, 3).EndWith(4).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 1, 2, 3, 4 }));
    }

    [Test]
    public void ShouldAppendMultipleValuesInOrder()
    {
        var results = new List<int>();
        Observable.Of(1, 2).EndWith(3, 4, 5).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 1, 2, 3, 4, 5 }));
    }

    [Test]
    public void ShouldEmitOnlyTheAppendedValuesForAnEmptySource()
    {
        var results = new List<int>();
        Observable.Empty<int>().EndWith(1, 2).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 1, 2 }));
    }

    [Test]
    public void ShouldMirrorSourceUnchangedWhenGivenNoValues()
    {
        var results = new List<int>();
        Observable.Of(1, 2, 3).EndWith().Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 1, 2, 3 }));
    }

    [Test]
    public void ShouldNotAppendAnythingIfTheSourceErrors()
    {
        var error = new InvalidOperationException("boom");
        var results = new List<int>();
        Exception? received = null;

        new Observable<int>(subscriber =>
        {
            subscriber.OnNext(1);
            subscriber.OnError(error);
        }).EndWith(2, 3).Subscribe(results.Add, onError: err => received = err);

        Assert.That(results, Is.EqualTo(new[] { 1 }));
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

        synchronousObservable.EndWith(-1).Take(3).Subscribe(_ => { });

        Assert.That(sideEffects, Is.EqualTo(new[] { 0, 1, 2 }));
    }
}
