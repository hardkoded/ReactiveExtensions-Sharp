namespace ReactiveExtensionsSharp.Operators;

/// <summary>Extension methods implementing the <c>switchMap</c> operator.</summary>
public static class SwitchMapOperator
{
    /// <summary>
    /// Projects each value from <paramref name="source"/> to an inner observable via <paramref name="project"/>
    /// and mirrors only the most recently projected inner observable. Whenever <paramref name="source"/>
    /// emits a new value, the previous inner observable's subscription is torn down (even if it is still
    /// live and emitting) before subscribing to the new one. The output completes once <paramref name="source"/>
    /// has completed and the current inner observable, if any, has also completed.
    /// </summary>
    /// <remarks>
    /// If <paramref name="project"/> throws, the exception is forwarded to the subscriber via <c>OnError</c>
    /// instead of propagating synchronously. An error from <paramref name="source"/> or from the active inner
    /// observable also terminates the output immediately via <c>OnError</c>.
    /// </remarks>
    /// <typeparam name="TSource">The type of values emitted by <paramref name="source"/>.</typeparam>
    /// <typeparam name="TResult">The type of values emitted by the inner observables produced by <paramref name="project"/>.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="project">A function that maps each source value to the inner observable to switch to.</param>
    /// <returns>An observable that emits the values of only the most recently projected inner observable.</returns>
    public static Observable<TResult> SwitchMap<TSource, TResult>(this Observable<TSource> source, Func<TSource, Observable<TResult>> project)
        => source.SwitchMap((value, _) => project(value));

    /// <summary>
    /// Projects each value from <paramref name="source"/>, along with its zero-based index, to an inner
    /// observable via <paramref name="project"/> and mirrors only the most recently projected inner observable.
    /// Whenever <paramref name="source"/> emits a new value, the previous inner observable's subscription is
    /// torn down (even if it is still live and emitting) before subscribing to the new one. The output
    /// completes once <paramref name="source"/> has completed and the current inner observable, if any, has
    /// also completed.
    /// </summary>
    /// <remarks>
    /// If <paramref name="project"/> throws, the exception is forwarded to the subscriber via <c>OnError</c>
    /// instead of propagating synchronously. An error from <paramref name="source"/> or from the active inner
    /// observable also terminates the output immediately via <c>OnError</c>.
    /// </remarks>
    /// <typeparam name="TSource">The type of values emitted by <paramref name="source"/>.</typeparam>
    /// <typeparam name="TResult">The type of values emitted by the inner observables produced by <paramref name="project"/>.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="project">A function that maps each source value and its index (starting at 0) to the inner observable to switch to.</param>
    /// <returns>An observable that emits the values of only the most recently projected inner observable.</returns>
    public static Observable<TResult> SwitchMap<TSource, TResult>(this Observable<TSource> source, Func<TSource, int, Observable<TResult>> project)
        => source.Operate<TSource, TResult>((src, subscriber) =>
        {
            var index = 0;
            var isSourceComplete = false;
            var innerSubscription = new SingleAssignmentDisposable();
            var hasInner = false;
            subscriber.Add(innerSubscription);

            void CheckComplete()
            {
                if (isSourceComplete && !hasInner)
                {
                    subscriber.OnCompleted();
                }
            }

            return src.SubscribeChild(
                subscriber,
                onNext: value =>
                {
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

                    // SingleAssignmentDisposable only overwrites; switchMap needs the previous (still-live)
                    // inner subscription actually torn down before starting the new one.
                    innerSubscription.Disposable?.Dispose();
                    hasInner = true;

                    // Built directly and assigned into the (already-registered-with-`subscriber`) slot BEFORE
                    // `inner.Subscribe` runs, so a fully-synchronous inner is stopped mid-loop by a downstream
                    // disposal instead of only after Subscribe returns. `innerSubscription` itself is a single,
                    // reused slot added to `subscriber` once for the operator's whole lifetime (see below), so no
                    // matching Remove is needed per switch — reassigning it is enough.
                    Subscriber<TResult>? newInnerSubscriber = null;
                    newInnerSubscriber = Subscriber.Create<TResult>(
                        onNext: subscriber.OnNext,
                        onError: subscriber.OnError,
                        onComplete: () =>
                        {
                            hasInner = false;
                            CheckComplete();
                        });
                    innerSubscription.Disposable = newInnerSubscriber;
                    inner.Subscribe(newInnerSubscriber);
                },
                onError: subscriber.OnError,
                onComplete: () =>
                {
                    isSourceComplete = true;
                    CheckComplete();
                });
        });
}
