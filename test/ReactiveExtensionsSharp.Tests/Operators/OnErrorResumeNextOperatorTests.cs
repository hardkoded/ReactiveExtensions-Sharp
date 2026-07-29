using ReactiveExtensionsSharp.Operators;

namespace ReactiveExtensionsSharp.Tests.Operators;

// Ported (non-marble subset) from rxjs 7.8.2 spec/operators/onErrorResumeNext-spec.ts, exercised via the
// pipeable operator form (source.OnErrorResumeNext(...)). The standalone creation-function form
// (Observable.OnErrorResumeNext(...)) already has thorough coverage in Observable.CreationTests.cs; this only
// needs to confirm the pipeable sugar correctly prepends source before delegating to it.
[TestFixture]
public class OnErrorResumeNextOperatorTests
{
    [Test]
    public void ShouldMoveToTheNextSourceOnErrorOrOnComplete()
    {
        var results = new List<int>();
        var completed = false;

        Observable.Concat(Observable.Of(1, 2), Observable.ThrowError<int>(() => new InvalidOperationException("boom")))
            .OnErrorResumeNext(Observable.Of(3, 4))
            .Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.EqualTo(new[] { 1, 2, 3, 4 }));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldReturnSourceUnchangedWhenGivenNoOtherSources()
    {
        var results = new List<int>();
        Observable.Of(1, 2, 3).OnErrorResumeNext().Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 1, 2, 3 }));
    }

    [Test]
    public void ShouldCompleteRatherThanErrorWhenSourceIsTheOnlyOneAndItErrors()
    {
        var completed = false;
        var errored = false;

        Observable.ThrowError<int>(() => new InvalidOperationException("boom"))
            .OnErrorResumeNext()
            .Subscribe(onError: _ => errored = true, onComplete: () => completed = true);

        Assert.That(errored, Is.False);
        Assert.That(completed, Is.True);
    }
}
