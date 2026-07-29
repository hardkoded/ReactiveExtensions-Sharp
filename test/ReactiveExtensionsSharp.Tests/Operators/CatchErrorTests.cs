using ReactiveExtensionsSharp.Operators;

namespace ReactiveExtensionsSharp.Tests.Operators;

// Ported from rxjs 7.8.2 spec/operators/catchError-spec.ts (non-marble cases).
[TestFixture]
public class CatchErrorTests
{
    [Test]
    public void ShouldCatchErrorAndReplaceWithAnObservable()
    {
        var results = new List<int>();
        var completed = false;
        Observable.ThrowError<int>(() => new InvalidOperationException("boom"))
            .CatchError(_ => Observable.Of(1, 2, 3))
            .Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.EqualTo(new[] { 1, 2, 3 }));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldMirrorTheSourceIfItDoesNotRaiseErrors()
    {
        var results = new List<int>();
        Observable.Of(1, 2, 3).CatchError(_ => Observable.Of(-1)).Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 1, 2, 3 }));
    }

    [Test]
    public void ShouldCompleteIfYouReturnEmpty()
    {
        var completed = false;
        Observable.ThrowError<int>(() => new InvalidOperationException("boom"))
            .CatchError(_ => Observable.Empty<int>())
            .Subscribe(onComplete: () => completed = true);

        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldRaiseErrorIfYouReturnThrowError()
    {
        var replacementError = new InvalidOperationException("replacement");
        Exception? received = null;
        Observable.ThrowError<int>(() => new InvalidOperationException("original"))
            .CatchError(_ => Observable.ThrowError<int>(() => replacementError))
            .Subscribe(onError: err => received = err);

        Assert.That(received, Is.SameAs(replacementError));
    }

    [Test]
    public void ShouldNeverTerminateIfYouReturnNever()
    {
        var completed = false;
        var errored = false;
        Observable.ThrowError<int>(() => new InvalidOperationException("boom"))
            .CatchError(_ => Observable.Never<int>())
            .Subscribe(onError: _ => errored = true, onComplete: () => completed = true);

        Assert.That(completed, Is.False);
        Assert.That(errored, Is.False);
    }

    [Test]
    public void ShouldPassTheErrorAsTheFirstArgument()
    {
        var error = new InvalidOperationException("boom");
        Exception? passedToSelector = null;
        Observable.ThrowError<int>(() => error)
            .CatchError(err =>
            {
                passedToSelector = err;
                return Observable.Empty<int>();
            })
            .Subscribe();

        Assert.That(passedToSelector, Is.SameAs(error));
    }

    [Test]
    public void ShouldCatchErrorsThrownFromWithinTheConstructor()
    {
        var results = new List<int>();
        new Observable<int>(_ => throw new InvalidOperationException("constructor boom"))
            .CatchError(_ => Observable.Of(1, 2))
            .Subscribe(results.Add);

        Assert.That(results, Is.EqualTo(new[] { 1, 2 }));
    }

    // Regression test for the disposal-cascade fix (see CLAUDE.md Learnings): a fully-synchronous,
    // self-checking source composed with CatchError and an early-completing Take must stop mid-loop, not just
    // once the whole synchronous call stack unwinds. This source never errors, so it exercises the "before the
    // first error" source subscription being registered as a child of the downstream subscriber.
    [Test]
    public void ShouldCascadeDisposalThroughTheSourceBeforeAnyError()
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

        source.CatchError(_ => Observable.Of(-1)).Take(3).Subscribe(_ => { });

        Assert.That(sideEffects, Is.EqualTo(new[] { 0, 1, 2 }));
    }

    // Same cascade property, but through the replacement observable subscribed to after the source errors —
    // proves the replacement (which reuses the downstream subscriber directly, see CatchError.cs) also stops a
    // fully-synchronous replacement mid-loop.
    [Test]
    public void ShouldCascadeDisposalThroughTheReplacementObservable()
    {
        var sideEffects = new List<int>();
        var replacement = new Observable<int>(subscriber =>
        {
            for (var i = 0; !subscriber.IsDisposed && i < 10; i++)
            {
                sideEffects.Add(i);
                subscriber.OnNext(i);
            }
        });

        Observable.ThrowError<int>(() => new InvalidOperationException("boom"))
            .CatchError(_ => replacement)
            .Take(3)
            .Subscribe(_ => { });

        Assert.That(sideEffects, Is.EqualTo(new[] { 0, 1, 2 }));
    }
}
