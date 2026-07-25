namespace RxSharp.Operators;

/// <summary>
/// Extension methods implementing the <c>mergeMap</c> operator.
/// </summary>
/// <remarks>
/// Unlike rxjs's <c>mergeMap(project, concurrent)</c>, this port always merges with unbounded concurrency:
/// there is no <c>concurrent</c> limit parameter, since Puppeteer's own usage (the motivating consumer of
/// this port) never needs to cap in-flight inner subscriptions.
/// </remarks>
public static class MergeMapOperator
{
    /// <summary>
    /// Projects each value from <paramref name="source"/> to an inner observable via <paramref name="project"/>,
    /// subscribes to every inner observable as soon as it is produced, and merges all of their emissions into
    /// the output. The output completes once <paramref name="source"/> has completed and every inner
    /// observable it produced has also completed.
    /// </summary>
    /// <remarks>
    /// If <paramref name="project"/> throws, the exception is forwarded to the subscriber via <c>OnError</c>
    /// instead of propagating synchronously. An error from <paramref name="source"/> or from any inner
    /// observable also terminates the output immediately via <c>OnError</c>.
    /// </remarks>
    /// <typeparam name="TSource">The type of values emitted by <paramref name="source"/>.</typeparam>
    /// <typeparam name="TResult">The type of values emitted by the inner observables produced by <paramref name="project"/>.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="project">A function that maps each source value to an inner observable to merge into the output.</param>
    /// <returns>An observable that merges the values of all inner observables produced from the source values.</returns>
    public static Observable<TResult> MergeMap<TSource, TResult>(this Observable<TSource> source, Func<TSource, Observable<TResult>> project)
        => source.MergeMap((value, _) => project(value));

    /// <summary>
    /// Projects each value from <paramref name="source"/>, along with its zero-based index, to an inner
    /// observable via <paramref name="project"/>, subscribes to every inner observable as soon as it is
    /// produced, and merges all of their emissions into the output. The output completes once
    /// <paramref name="source"/> has completed and every inner observable it produced has also completed.
    /// </summary>
    /// <remarks>
    /// If <paramref name="project"/> throws, the exception is forwarded to the subscriber via <c>OnError</c>
    /// instead of propagating synchronously. An error from <paramref name="source"/> or from any inner
    /// observable also terminates the output immediately via <c>OnError</c>.
    /// </remarks>
    /// <typeparam name="TSource">The type of values emitted by <paramref name="source"/>.</typeparam>
    /// <typeparam name="TResult">The type of values emitted by the inner observables produced by <paramref name="project"/>.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="project">A function that maps each source value and its index (starting at 0) to an inner observable to merge into the output.</param>
    /// <returns>An observable that merges the values of all inner observables produced from the source values.</returns>
    public static Observable<TResult> MergeMap<TSource, TResult>(this Observable<TSource> source, Func<TSource, int, Observable<TResult>> project)
        => source.Operate<TSource, TResult>((src, subscriber) =>
        {
            var index = 0;
            var isSourceComplete = false;
            var activeCount = 0;

            void CheckComplete()
            {
                if (isSourceComplete && activeCount == 0)
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

                    activeCount++;

                    // Built directly (not via Subscribe's return value) and registered with `subscriber` before
                    // `inner.Subscribe` runs, so a fully-synchronous inner is stopped mid-loop by a downstream
                    // disposal instead of only after Subscribe returns. Removed again once it naturally completes,
                    // so `subscriber`'s finalizer list doesn't grow unboundedly across a long-running merge.
                    Subscriber<TResult>? innerSubscriber = null;
                    innerSubscriber = Subscriber.Create<TResult>(
                        onNext: subscriber.OnNext,
                        onError: subscriber.OnError,
                        onComplete: () =>
                        {
                            activeCount--;
                            subscriber.Remove(innerSubscriber!);
                            CheckComplete();
                        });
                    subscriber.Add(innerSubscriber);
                    inner.Subscribe(innerSubscriber);
                },
                onError: subscriber.OnError,
                onComplete: () =>
                {
                    isSourceComplete = true;
                    CheckComplete();
                });
        });
}
