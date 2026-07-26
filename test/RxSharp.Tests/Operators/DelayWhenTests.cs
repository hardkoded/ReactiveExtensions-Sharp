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

    // rxjs's `delayWhen` also accepts an optional second `subscriptionDelay` observable that delays subscribing
    // to the source itself, separate from the per-value `delayDurationSelector`. This codebase's `DelayWhen` only
    // has the two delayDurationSelector overloads (indexed and non-indexed) — no subscriptionDelay overload
    // exists to port that upstream case against. Noted as a feature gap rather than silently skipped.
    [Test]
    public void ShouldPropagateErrorsWhenTheSelectorFunctionThrows()
    {
        var error = new InvalidOperationException("boom");
        Exception? received = null;

        Observable.Of(1, 2).DelayWhen<int, Unit>((_, _) => throw error).Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldCallTheSelectorWithIndicesStartingAtZero()
    {
        var indices = new List<int>();
        var completed = false;

        Observable.Of("a", "b", "c").DelayWhen((value, index) =>
        {
            indices.Add(index);
            return Observable.Of(Unit.Default);
        }).Subscribe(onComplete: () => completed = true);

        Assert.That(indices, Is.EqualTo(new[] { 0, 1, 2 }));
        Assert.That(completed, Is.True);
    }
}
