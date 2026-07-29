using ReactiveExtensionsSharp.Operators;

namespace ReactiveExtensionsSharp.Tests.Operators;

// Ported from rxjs 7.8.2 spec/operators/tap-spec.ts (non-marble cases).
[TestFixture]
public class TapTests
{
    [Test]
    public void ShouldMirrorMultipleValuesAndComplete()
    {
        var results = new List<int>();
        var completed = false;
        Observable.Of(1, 2, 3).Tap().Subscribe(results.Add, onComplete: () => completed = true);

        Assert.That(results, Is.EqualTo(new[] { 1, 2, 3 }));
        Assert.That(completed, Is.True);
    }

    [Test]
    public void ShouldNextWithACallback()
    {
        var sideEffects = new List<int>();
        var results = new List<int>();
        Observable.Of(1, 2, 3).Tap(sideEffects.Add).Subscribe(results.Add);

        Assert.That(sideEffects, Is.EqualTo(new[] { 1, 2, 3 }));
        Assert.That(results, Is.EqualTo(new[] { 1, 2, 3 }));
    }

    [Test]
    public void ShouldHandleAnErrorWithACallback()
    {
        var error = new InvalidOperationException("boom");
        Exception? sideEffectError = null;
        Exception? received = null;
        Observable.ThrowError<int>(() => error).Tap(onError: err => sideEffectError = err).Subscribe(onError: err => received = err);

        Assert.That(sideEffectError, Is.SameAs(error));
        Assert.That(received, Is.SameAs(error));
    }

    [Test]
    public void ShouldHandleCompleteWithACallback()
    {
        var completedSideEffect = false;
        Observable.Of(1).Tap(onComplete: () => completedSideEffect = true).Subscribe();

        Assert.That(completedSideEffect, Is.True);
    }

    [Test]
    public void ShouldRaiseErrorIfNextHandlerRaisesError()
    {
        Exception? received = null;
        Observable.Of("hi").Tap(_ => throw new InvalidOperationException("bad")).Subscribe(onError: err => received = err);

        Assert.That(received, Is.Not.Null);
        Assert.That(received!.Message, Is.EqualTo("bad"));
    }

    [Test]
    public void ShouldRaiseErrorIfErrorHandlerRaisesError()
    {
        Exception? received = null;
        Observable.ThrowError<string>(() => new InvalidOperationException("ops"))
            .Tap(onError: _ => throw new InvalidOperationException("bad"))
            .Subscribe(onError: err => received = err);

        Assert.That(received, Is.Not.Null);
        Assert.That(received!.Message, Is.EqualTo("bad"));
    }

    [Test]
    public void ShouldRaiseErrorIfCompleteHandlerRaisesError()
    {
        Exception? received = null;
        Observable.Empty<int>()
            .Tap(onComplete: () => throw new InvalidOperationException("bad"))
            .Subscribe(onError: err => received = err);

        Assert.That(received, Is.Not.Null);
        Assert.That(received!.Message, Is.EqualTo("bad"));
    }
}
