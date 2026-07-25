namespace RxSharp.Operators;

/// <summary>Extension methods implementing the <c>exhaustMap</c> operator.</summary>
public static class ExhaustMapOperator
{
    /// <summary>
    /// Projects each value from <paramref name="source"/> to an inner observable via <paramref name="project"/>,
    /// but only while no previously projected inner observable is still active: while one is active, new
    /// <paramref name="source"/> values are silently ignored (not queued) rather than merged or switched to.
    /// Once the active inner observable completes, the next <paramref name="source"/> value is free to start a
    /// new one. The output completes once <paramref name="source"/> has completed and the current inner
    /// observable, if any, has also completed.
    /// </summary>
    /// <remarks>
    /// If <paramref name="project"/> throws, the exception is forwarded to the subscriber via <c>OnError</c>
    /// instead of propagating synchronously. An error from <paramref name="source"/> or from the active inner
    /// observable also terminates the output immediately via <c>OnError</c>. The index passed to <paramref name="project"/>
    /// only advances for values that are actually accepted (i.e. not ignored while an inner observable is active).
    /// </remarks>
    /// <typeparam name="TSource">The type of values emitted by <paramref name="source"/>.</typeparam>
    /// <typeparam name="TResult">The type of values emitted by the inner observables produced by <paramref name="project"/>.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="project">A function that maps each accepted source value to the inner observable to subscribe to.</param>
    /// <returns>An observable that emits the values of each accepted inner observable, ignoring source values that arrive while one is still active.</returns>
    public static Observable<TResult> ExhaustMap<TSource, TResult>(this Observable<TSource> source, Func<TSource, Observable<TResult>> project)
        => source.ExhaustMap((value, _) => project(value));

    /// <summary>
    /// Projects each value from <paramref name="source"/>, along with its zero-based acceptance index, to an
    /// inner observable via <paramref name="project"/>, but only while no previously projected inner observable
    /// is still active: while one is active, new <paramref name="source"/> values are silently ignored (not
    /// queued) rather than merged or switched to. Once the active inner observable completes, the next
    /// <paramref name="source"/> value is free to start a new one. The output completes once <paramref name="source"/>
    /// has completed and the current inner observable, if any, has also completed.
    /// </summary>
    /// <remarks>
    /// If <paramref name="project"/> throws, the exception is forwarded to the subscriber via <c>OnError</c>
    /// instead of propagating synchronously. An error from <paramref name="source"/> or from the active inner
    /// observable also terminates the output immediately via <c>OnError</c>. The index only advances for values
    /// that are actually accepted (i.e. not ignored while an inner observable is active) — so it is not the same
    /// as the emission index of <paramref name="source"/> itself.
    /// </remarks>
    /// <typeparam name="TSource">The type of values emitted by <paramref name="source"/>.</typeparam>
    /// <typeparam name="TResult">The type of values emitted by the inner observables produced by <paramref name="project"/>.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="project">A function that maps each accepted source value and its acceptance index (starting at 0) to the inner observable to subscribe to.</param>
    /// <returns>An observable that emits the values of each accepted inner observable, ignoring source values that arrive while one is still active.</returns>
    public static Observable<TResult> ExhaustMap<TSource, TResult>(this Observable<TSource> source, Func<TSource, int, Observable<TResult>> project)
        => source.Operate<TSource, TResult>((src, subscriber) =>
        {
            var index = 0;
            var isSourceComplete = false;
            var hasInner = false;
            var innerSubscription = new SingleAssignmentDisposable();
            subscriber.Add(innerSubscription);

            return src.SubscribeChild(
                subscriber,
                onNext: value =>
                {
                    if (hasInner)
                    {
                        return;
                    }

                    Observable<TResult> inner;
                    try
                    {
                        inner = project(value, index++);
                    }
                    catch (Exception ex)
                    {
                        subscriber.OnError(ex);
                        return;
                    }

                    hasInner = true;

                    // Built directly and assigned into the (already-registered-with-`subscriber`) slot BEFORE
                    // `inner.Subscribe` runs, so a fully-synchronous inner is stopped mid-loop by a downstream
                    // disposal instead of only after Subscribe returns. `innerSubscription` is a single, reused
                    // slot added to `subscriber` once for the operator's whole lifetime, so no matching Remove
                    // is needed per accepted value — reassigning it is enough.
                    Subscriber<TResult>? newInnerSubscriber = null;
                    newInnerSubscriber = Subscriber.Create<TResult>(
                        onNext: subscriber.OnNext,
                        onError: subscriber.OnError,
                        onComplete: () =>
                        {
                            hasInner = false;
                            if (isSourceComplete)
                            {
                                subscriber.OnCompleted();
                            }
                        });
                    innerSubscription.Disposable = newInnerSubscriber;
                    inner.Subscribe(newInnerSubscriber);
                },
                onError: subscriber.OnError,
                onComplete: () =>
                {
                    isSourceComplete = true;
                    if (!hasInner)
                    {
                        subscriber.OnCompleted();
                    }
                });
        });
}
