using RxSharp.Operators;
using RxSharp.Subjects;

namespace RxSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/withLatestFrom-spec.ts. The interop-observable
// tests (arrays/promises/lowercase-o observables as implicit ObservableInput) are not portable: this port has
// no such implicit conversions, so only real Observable<T> sources are exercised.
[TestFixture]
public class WithLatestFromTests
{
    [Test]
    public void ShouldCombineWithTheLatestValueUsingAProjection()
    {
        var source = new Subject<string>();
        var other = new Subject<int>();
        var results = new List<string>();

        source.AsObservable().WithLatestFrom(other.AsObservable(), (a, b) => a + b).Subscribe(results.Add);

        source.OnNext("a"); // other has no value yet -- ignored
        other.OnNext(1);
        source.OnNext("b");
        other.OnNext(2);
        other.OnNext(3);
        source.OnNext("c");

        Assert.That(results, Is.EqualTo(new[] { "b1", "c3" }));
    }

    [Test]
    public void ShouldMergeTheValueWithTheLatestValuesFromMultipleObservablesIntoATuple()
    {
        var source = new Subject<string>();
        var other = new Subject<string>();
        var results = new List<(string Source, string Other)>();

        source.AsObservable().WithLatestFrom(other.AsObservable()).Subscribe(results.Add);

        other.OnNext("f");
        source.OnNext("b");
        other.OnNext("g");
        source.OnNext("c");

        Assert.That(results, Is.EqualTo(new[] { ("b", "f"), ("c", "g") }));
    }

    [Test]
    public void ShouldCombineWithTheLatestValuesFromMultipleObservablesAsAList()
    {
        var source = new Subject<string>();
        var e2 = new Subject<string>();
        var e3 = new Subject<string>();
        var results = new List<IReadOnlyList<string>>();

        source.AsObservable().WithLatestFrom(e2.AsObservable(), e3.AsObservable()).Subscribe(results.Add);

        e2.OnNext("f");
        e3.OnNext("j");
        source.OnNext("b");
        e2.OnNext("g");
        e3.OnNext("k");
        source.OnNext("c");

        Assert.That(results.Select(r => string.Join(",", r)), Is.EqualTo(new[] { "b,f,j", "c,g,k" }));
    }

    [Test]
    public void ShouldNotEmitUntilEveryOtherObservableHasEmittedAtLeastOnce()
    {
        var source = new Subject<int>();
        var e2 = new Subject<int>();
        var e3 = new Subject<int>();
        var results = new List<IReadOnlyList<int>>();

        source.AsObservable().WithLatestFrom(e2.AsObservable(), e3.AsObservable()).Subscribe(results.Add);

        source.OnNext(1);
        e2.OnNext(10);
        source.OnNext(2);
        e3.OnNext(100);
        source.OnNext(3);

        Assert.That(results.Count, Is.EqualTo(1));
        Assert.That(results[0], Is.EqualTo(new[] { 3, 10, 100 }));
    }

    [Test]
    public void ShouldHandleAnEmptySource()
    {
        var completed = false;
        var results = new List<IReadOnlyList<int>>();
        Observable.Empty<int>().WithLatestFrom(Observable.Of(1), Observable.Of(2))
            .Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.Empty);
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldForwardErrorsFromTheSource()
    {
        var error = new InvalidOperationException("boo-hoo");
        Exception? received = null;
        var results = new List<int[]>();

        var e2 = new Subject<int>();
        var source = new Subject<int>();

        source.AsObservable().WithLatestFrom(e2.AsObservable()).Subscribe(pair => results.Add(new[] { pair.Source, pair.Other }), onError: err => received = err);

        e2.OnNext(1);
        source.OnNext(1);
        source.OnError(error);

        Assert.That(results.Select(r => (r[0], r[1])), Is.EqualTo(new[] { (1, 1) }));
        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldForwardAnErrorFromAnOtherObservableToo()
    {
        var error = new InvalidOperationException("other boom");
        Exception? received = null;
        var e2 = new Subject<int>();
        var source = new Subject<int>();

        source.AsObservable().WithLatestFrom(e2.AsObservable()).Subscribe(onError: err => received = err);

        e2.OnNext(1);
        e2.OnError(error);

        // Matches rxjs: an error from any input -- not just the primary source -- ends the whole output.
        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldWorkWithSynchronousObservables()
    {
        var results = new List<int[]>();
        Observable.Of(1, 2, 3).WithLatestFrom(Observable.Of(4, 5))
            .Subscribe(pair => results.Add(new[] { pair.Source, pair.Other }));

        Assert.That(results.Select(r => (r[0], r[1])), Is.EqualTo(new[] { (1, 5), (2, 5), (3, 5) }));
    }

    [Test]
    public void ShouldNotBeAffectedByOtherCompleting()
    {
        var e2 = new Subject<int>();
        var source = new Subject<int>();
        var results = new List<int>();
        var completed = false;

        source.AsObservable().WithLatestFrom(e2.AsObservable(), (a, b) => a + b)
            .Subscribe(results.Add, onComplete: () => completed = true);

        e2.OnNext(10);
        e2.OnCompleted();
        source.OnNext(1);
        source.OnNext(2);
        source.OnCompleted();

        Assert.That(results, Is.EqualTo(new[] { 11, 12 }));
        Assert.That(completed, Is.True);
    }
}
