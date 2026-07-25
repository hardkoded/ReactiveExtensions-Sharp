using RxSharp.Operators;
using RxSharp.Subjects;

namespace RxSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/delayWhen-spec.ts.
[TestFixture]
public class DelayWhenTests
{
    [Test]
    public void ShouldDelayEachValueUntilItsDurationObservableEmits()
    {
        var durations = new Dictionary<int, Subject<Unit>>
        {
            [1] = new(),
            [2] = new(),
        };
        var results = new List<int>();

        Observable.Of(1, 2).DelayWhen(value => durations[value].AsObservable()).Subscribe(results.Add);

        Assert.That(results, Is.Empty);

        durations[2].OnNext(Unit.Default);
        Assert.That(results, Is.EqualTo(new[] { 2 }));

        durations[1].OnNext(Unit.Default);
        Assert.That(results, Is.EqualTo(new[] { 2, 1 }));
    }

    [Test]
    public void ShouldSwallowAValueWhoseDurationObservableCompletesWithoutEmitting()
    {
        var duration = new Subject<Unit>();
        var results = new List<int>();
        var completed = false;

        Observable.Of(1).DelayWhen(_ => duration.AsObservable()).Subscribe(results.Add, onComplete: () => completed = true);

        duration.OnCompleted();

        Assert.That(results, Is.Empty);
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldPropagateErrorsFromTheDurationObservable()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;
        Observable.Of(1).DelayWhen(_ => Observable.ThrowError<Unit>(() => error)).Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }
}
