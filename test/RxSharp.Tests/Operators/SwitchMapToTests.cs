using RxSharp.Operators;
using RxSharp.Subjects;

namespace RxSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/switchMapTo-spec.ts.
[TestFixture]
public class SwitchMapToTests
{
    [Test]
    public void ShouldSwitchToAFreshSubscriptionOfTheSameInnerOnEverySourceValue()
    {
        var source = new Subject<int>();
        var results = new List<string>();
        var innerSubscriptions = 0;

        var inner = Observable.Defer(() =>
        {
            innerSubscriptions++;
            return Observable.Of($"inner {innerSubscriptions}");
        });

        source.AsObservable().SwitchMapTo(inner).Subscribe(results.Add);

        source.OnNext(1);
        source.OnNext(2);

        Assert.That(results, Is.EqualTo(new[] { "inner 1", "inner 2" }));
        Assert.That(innerSubscriptions, Is.EqualTo(2));
    }

    [Test]
    public void ShouldForwardAnErrorFromTheInnerObservable()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;

        Observable.Of(1).SwitchMapTo(Observable.ThrowError<int>(() => error)).Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldCompleteWithoutSubscribingToTheInnerOnAnEmptySource()
    {
        var innerSubscribed = false;
        var completed = false;
        var inner = new Observable<int>(_ => innerSubscribed = true);

        Observable.Empty<int>().SwitchMapTo(inner).Subscribe(onComplete: () => completed = true);

        Assert.That(innerSubscribed, Is.False);
        Assert.That(completed, Is.True);
    }
}
