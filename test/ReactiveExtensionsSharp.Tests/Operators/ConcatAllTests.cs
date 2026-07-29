using ReactiveExtensionsSharp.Operators;

namespace ReactiveExtensionsSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/concatAll-spec.ts.
[TestFixture]
public class ConcatAllTests
{
    [Test]
    public void ShouldConcatenateInnerObservablesInOrder()
    {
        var results = new List<int>();
        Observable.Of(Observable.Of(1, 2), Observable.Of(3, 4)).ConcatAll().Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 1, 2, 3, 4 }));
    }

    [Test]
    public void ShouldOnlySubscribeToTheNextInnerOnceThePreviousCompletes()
    {
        var subscribeOrder = new List<int>();
        Observable<int> Track(int n) => Observable.Defer(() =>
        {
            subscribeOrder.Add(n);
            return Observable.Of(n);
        });

        Observable.Of(Track(1), Track(2)).ConcatAll().Subscribe();

        Assert.That(subscribeOrder, Is.EqualTo(new[] { 1, 2 }));
    }

    [Test]
    public void ShouldForwardAnErrorFromAnInnerObservable()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;

        Observable.Of(Observable.ThrowError<int>(() => error)).ConcatAll().Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }
}
