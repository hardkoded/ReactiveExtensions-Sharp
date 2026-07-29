using System.Reflection;
using ReactiveExtensionsSharp.Operators;
using ReactiveExtensionsSharp.Subjects;

namespace ReactiveExtensionsSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/groupBy-spec.ts. The duration-selector tests are
// adapted (rxjs uses `skip(1)`/`skip(2)` on the group itself, which ReactiveExtensionsSharp does not implement yet) to instead
// use an externally controlled duration Subject that closes the group on demand.
[TestFixture]
public class GroupByTests
{
    [Test]
    public void ShouldGroupValues()
    {
        var expectedGroups = new List<(int Key, List<int> Values)> { (1, new List<int> { 1, 3 }), (0, new List<int> { 2 }) };
        var completed = false;

        Observable.Of(1, 2, 3).GroupBy(x => x % 2).Subscribe(
            group =>
            {
                var expectedGroup = expectedGroups[0];
                expectedGroups.RemoveAt(0);
                Assert.That(group.Key, Is.EqualTo(expectedGroup.Key));

                group.Subscribe(x => Assert.That(x, Is.EqualTo(PopFirst(expectedGroup.Values))));
            },
            onComplete: () => completed = true);

        Assert.That(completed, Is.True);
        Assert.That(expectedGroups, Is.Empty);
    }

    [Test]
    public void ShouldGroupValuesWithAnElementSelector()
    {
        var expectedGroups = new List<(int Key, List<string> Values)> { (1, new List<string> { "1!", "3!" }), (0, new List<string> { "2!" }) };
        var completed = false;

        Observable.Of(1, 2, 3).GroupBy(x => x % 2, x => x + "!").Subscribe(
            group =>
            {
                var expectedGroup = expectedGroups[0];
                expectedGroups.RemoveAt(0);
                Assert.That(group.Key, Is.EqualTo(expectedGroup.Key));

                group.Subscribe(x => Assert.That(x, Is.EqualTo(PopFirst(expectedGroup.Values))));
            },
            onComplete: () => completed = true);

        Assert.That(completed, Is.True);
        Assert.That(expectedGroups, Is.Empty);
    }

    [Test]
    public void ShouldStartANewGroupAfterTheDurationSelectorClosesThePreviousOne()
    {
        var source = new Subject<int>();
        var duration = new Subject<Unit>();
        var groups = new List<(int Key, List<int> Values)>();

        source.AsObservable().GroupBy(x => x % 2, x => x, _ => duration.AsObservable()).Subscribe(group =>
        {
            var values = new List<int>();
            group.Subscribe(values.Add);
            groups.Add((group.Key, values));
        });

        source.OnNext(1);
        source.OnNext(3);

        Assert.That(groups, Has.Count.EqualTo(1));
        Assert.That(groups[0].Values, Is.EqualTo(new[] { 1, 3 }));

        duration.OnNext(Unit.Default);
        source.OnNext(5);

        Assert.That(groups, Has.Count.EqualTo(2), "a value with the same key after the group closed should start a new group");
        Assert.That(groups[1].Key, Is.EqualTo(1));
        Assert.That(groups[1].Values, Is.EqualTo(new[] { 5 }));
    }

    [Test]
    public void ShouldErrorAllOpenGroupsWhenTheKeySelectorThrows()
    {
        var error = new InvalidOperationException("boom");
        var invoked = 0;
        Exception? outerError = null;
        var groupErrors = new List<Exception>();

        Observable.Of(1, 2, 3).GroupBy(x =>
        {
            invoked++;
            if (invoked == 3)
            {
                throw error;
            }

            return x % 2;
        }).Subscribe(
            group => group.Subscribe(onError: err => groupErrors.Add(err)),
            onError: err => outerError = err);

        Assert.That(outerError, Is.SameAs(error));
        Assert.That(groupErrors, Has.Count.EqualTo(2), "both still-open groups (key 1 and key 0) should also error");
        Assert.That(groupErrors, Is.All.SameAs(error));
    }

    [Test]
    public void ShouldErrorAllOpenGroupsWhenTheSourceErrors()
    {
        var error = new InvalidOperationException("boom");
        Exception? outerError = null;
        var groupErrors = new List<Exception>();

        Observable.Concat(Observable.From(new[] { 1, 2 }), Observable.ThrowError<int>(() => error))
            .GroupBy(x => x % 2)
            .Subscribe(
                group => group.Subscribe(onError: err => groupErrors.Add(err)),
                onError: err => outerError = err);

        Assert.That(outerError, Is.SameAs(error));
        Assert.That(groupErrors, Has.Count.EqualTo(2));
        Assert.That(groupErrors, Is.All.SameAs(error));
    }

    [Test]
    public void ShouldCompleteAllOpenGroupsWhenTheSourceCompletes()
    {
        var completedGroups = new List<int>();
        var outerCompleted = false;

        Observable.Of(1, 2, 3).GroupBy(x => x % 2).Subscribe(
            group => group.Subscribe(onComplete: () => completedGroups.Add(group.Key)),
            onComplete: () => outerCompleted = true);

        Assert.That(outerCompleted, Is.True);
        Assert.That(completedGroups, Is.EquivalentTo(new[] { 1, 0 }));
    }

    // Regression test for the disposal-cascade fix (see CLAUDE.md Learnings): a fully-synchronous, self-checking
    // source composed with GroupBy and an early-completing Take on the *outer* stream of groups must stop
    // mid-loop, not just once the whole synchronous call stack unwinds. Every value gets a distinct (counter
    // derived) key, matching the "key cardinality could be unbounded" scenario CLAUDE.md calls out explicitly.
    [Test]
    public void ShouldCascadeDisposalThroughTheOuterGroupStream()
    {
        var sideEffects = new List<int>();
        var source = new Observable<int>(subscriber =>
        {
            for (var i = 0; !subscriber.IsDisposed && i < 10; i++)
            {
                sideEffects.Add(i);
                subscriber.OnNext(i);
            }
        });

        source.GroupBy(x => x).Take(3).Subscribe(_ => { });

        Assert.That(sideEffects, Is.EqualTo(new[] { 0, 1, 2 }));
    }

    // Regression test proving the per-key duration-notifier subscription is Remove()'d from the outer
    // subscriber's finalizer list once its group closes, instead of accumulating forever — the exact "unbounded
    // key cardinality" leak scenario CLAUDE.md calls out for GroupBy. Every source value gets its own key and
    // its own duration notifier that emits-then-completes synchronously (closing the group immediately), so
    // after many values the finalizer list must stay small, not grow proportionally with the number of groups.
    [Test]
    public void ShouldNotLeakDurationNotifierSubscriptionsAcrossManyGroups()
    {
        var source = new Subject<int>();
        var closed = 0;

        var subscription = source.AsObservable()
            .GroupBy(x => x, x => x, _ => Observable.Of(Unit.Default))
            .Subscribe(group => group.Subscribe(onComplete: () => closed++));

        for (var i = 0; i < 500; i++)
        {
            source.OnNext(i);
        }

        var finalizersField = typeof(Subscription).GetField("_finalizers", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var finalizers = (List<IDisposable>?)finalizersField.GetValue(subscription);

        Assert.That(closed, Is.EqualTo(500));
        Assert.That(
            finalizers is null ? 0 : finalizers.Count,
            Is.LessThan(10),
            "duration-notifier subscriptions must be Remove()'d once their group closes, not left to accumulate forever");
    }

    private static T PopFirst<T>(List<T> values)
    {
        var value = values[0];
        values.RemoveAt(0);
        return value;
    }
}
