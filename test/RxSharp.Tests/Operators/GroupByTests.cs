using RxSharp.Operators;
using RxSharp.Subjects;

namespace RxSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/groupBy-spec.ts. The duration-selector tests are
// adapted (rxjs uses `skip(1)`/`skip(2)` on the group itself, which RxSharp does not implement yet) to instead
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

    private static T PopFirst<T>(List<T> values)
    {
        var value = values[0];
        values.RemoveAt(0);
        return value;
    }
}
