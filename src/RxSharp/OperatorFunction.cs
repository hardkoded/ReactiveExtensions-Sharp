namespace RxSharp;

/// <summary>A function that transforms an <see cref="Observable{TSource}"/> into an <see cref="Observable{TResult}"/>, e.g. the shape of a bound operator ready to apply via <see cref="OperatorHelper.Pipe{TSource, TResult}"/>.</summary>
/// <typeparam name="TSource">The element type of the source observable.</typeparam>
/// <typeparam name="TResult">The element type of the resulting observable.</typeparam>
/// <param name="source">The source observable to transform.</param>
/// <returns>The transformed observable.</returns>
public delegate Observable<TResult> OperatorFunction<TSource, TResult>(Observable<TSource> source);

/// <summary>An <see cref="OperatorFunction{TSource, TResult}"/> whose source and result element types are the same, e.g. <c>Filter</c> or <c>Take</c>.</summary>
/// <typeparam name="T">The element type shared by the source and result.</typeparam>
/// <param name="source">The source observable to transform.</param>
/// <returns>The transformed observable.</returns>
public delegate Observable<T> MonoTypeOperatorFunction<T>(Observable<T> source);

/// <summary>The building block every operator is written against. Mirrors rxjs's <c>operate()</c> helper.</summary>
public static class OperatorHelper
{
    /// <summary>
    /// Creates a new <see cref="Observable{TResult}"/> whose subscribe logic is <paramref name="init"/>, given the
    /// upstream <paramref name="source"/> and the downstream <see cref="Subscriber{TResult}"/> to push notifications to.
    /// This is the low-level primitive every built-in operator is implemented on top of.
    /// </summary>
    /// <typeparam name="TSource">The element type of the source observable.</typeparam>
    /// <typeparam name="TResult">The element type of the resulting observable.</typeparam>
    /// <param name="source">The upstream source observable.</param>
    /// <param name="init">Called at subscribe time with the source and the downstream subscriber; returns the teardown logic for the subscription, if any.</param>
    /// <returns>The new, operator-defined observable.</returns>
    public static Observable<TResult> Operate<TSource, TResult>(
        this Observable<TSource> source,
        Func<Observable<TSource>, Subscriber<TResult>, IDisposable?> init)
        => new Observable<TResult>(subscriber => init(source, subscriber));

    /// <summary>Applies an <see cref="OperatorFunction{TSource, TResult}"/> to <paramref name="source"/>. Equivalent to calling <paramref name="op"/> directly; exists to let operators be composed in a left-to-right, method-chained style.</summary>
    /// <typeparam name="TSource">The element type of the source observable.</typeparam>
    /// <typeparam name="TResult">The element type of the resulting observable.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="op">The operator function to apply.</param>
    /// <returns>The result of applying <paramref name="op"/> to <paramref name="source"/>.</returns>
    public static Observable<TResult> Pipe<TSource, TResult>(this Observable<TSource> source, OperatorFunction<TSource, TResult> op)
        => op(source);

    /// <summary>
    /// Subscribes to <paramref name="source"/> with a fresh inner <see cref="Subscriber{TSource}"/>, registered as a
    /// child of <paramref name="downstream"/> <em>before</em> the subscribe call runs. This is what lets a downstream
    /// disposal (an early-completing operator further down the chain, or an external unsubscribe) cascade up and stop
    /// <paramref name="source"/> immediately — including mid-loop for a fully-synchronous, self-checking source —
    /// instead of only after the whole synchronous call stack has already unwound. See CLAUDE.md's Learnings for the
    /// full story of the bug this fixes.
    /// </summary>
    /// <remarks>
    /// Only use this for an operator with exactly ONE inner subscription for its whole lifetime. An operator that
    /// creates a new inner subscription per value or per cycle (`MergeMap`, `SwitchMap`, `Debounce`, `Window*`, ...)
    /// needs to also call <see cref="Subscription.Remove"/> once each inner subscription naturally ends, or the
    /// downstream subscriber's finalizer list grows unboundedly — this helper does not do that for you.
    /// </remarks>
    /// <typeparam name="TSource">The element type of the source observable.</typeparam>
    /// <typeparam name="TResult">The element type of the downstream subscriber (usually unrelated to the callback signatures, just needed to type <paramref name="downstream"/>).</typeparam>
    /// <param name="source">The source observable to subscribe to.</param>
    /// <param name="downstream">The downstream subscriber the new inner subscriber is registered as a child of.</param>
    /// <param name="onNext">Called for each value from <paramref name="source"/>.</param>
    /// <param name="onError">Called if <paramref name="source"/> errors.</param>
    /// <param name="onComplete">Called when <paramref name="source"/> completes.</param>
    /// <returns>The inner subscriber, already subscribed and already registered as a child of <paramref name="downstream"/>.</returns>
    public static IDisposable SubscribeChild<TSource, TResult>(
        this Observable<TSource> source,
        Subscriber<TResult> downstream,
        Action<TSource>? onNext = null,
        Action<Exception>? onError = null,
        Action? onComplete = null)
    {
        var innerSubscriber = Subscriber.Create(onNext, onError, onComplete);
        downstream.Add(innerSubscriber);
        return source.Subscribe(innerSubscriber);
    }
}
