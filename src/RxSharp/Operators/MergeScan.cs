namespace RxSharp.Operators;

/// <summary>Extension methods implementing the <c>mergeScan</c> operator.</summary>
public static class MergeScanOperator
{
    /// <summary>
    /// Applies <paramref name="accumulator"/> to each value emitted by <paramref name="source"/>, along with
    /// its zero-based index, subscribing to the inner observable it returns and merging its emissions into the
    /// output. The last value produced by an inner observable becomes the new accumulated state, which is
    /// passed as <c>acc</c> to the next call of <paramref name="accumulator"/>. The output completes once
    /// <paramref name="source"/> has completed and every inner observable has also completed.
    /// </summary>
    /// <remarks>
    /// Like <see cref="MergeMapOperator"/>, inner observables are merged with unbounded concurrency: more than
    /// one accumulator-produced observable can be active at the same time, so the retained state is updated in
    /// whatever order inner values actually arrive rather than in source-emission order.
    /// If <paramref name="accumulator"/> throws, the exception is forwarded to the subscriber via <c>OnError</c>
    /// instead of propagating synchronously. An error from <paramref name="source"/> or from any inner
    /// observable also terminates the output immediately via <c>OnError</c>.
    /// </remarks>
    /// <typeparam name="TSource">The type of values emitted by <paramref name="source"/>.</typeparam>
    /// <typeparam name="TAcc">The type of the accumulated state, of the values emitted by the inner observables, and of the output.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="accumulator">
    /// A function called with the current accumulated state, the source value, and its index (starting at 0);
    /// returns an inner observable whose values become the new accumulated state.
    /// </param>
    /// <param name="seed">The initial accumulated state, used for the first call to <paramref name="accumulator"/>.</param>
    /// <returns>An observable of the values produced by the accumulator's inner observables.</returns>
    public static Observable<TAcc> MergeScan<TSource, TAcc>(this Observable<TSource> source, Func<TAcc, TSource, int, Observable<TAcc>> accumulator, TAcc seed)
        => source.Operate<TSource, TAcc>((src, subscriber) =>
        {
            var state = seed;
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

            return src.Subscribe(
                onNext: value =>
                {
                    Observable<TAcc> inner;
                    try
                    {
                        inner = accumulator(state, value, index++);
                    }
                    catch (Exception ex)
                    {
                        subscriber.OnError(ex);
                        return;
                    }

                    activeCount++;
                    var innerSubscription = inner.Subscribe(
                        onNext: result =>
                        {
                            state = result;
                            subscriber.OnNext(result);
                        },
                        onError: subscriber.OnError,
                        onComplete: () =>
                        {
                            activeCount--;
                            CheckComplete();
                        });
                    subscriber.Add(innerSubscription);
                },
                onError: subscriber.OnError,
                onComplete: () =>
                {
                    isSourceComplete = true;
                    CheckComplete();
                });
        });

    /// <summary>
    /// Applies <paramref name="accumulator"/> to each value emitted by <paramref name="source"/>, subscribing
    /// to the inner observable it returns and merging its emissions into the output. The last value produced
    /// by an inner observable becomes the new accumulated state, which is passed as <c>acc</c> to the next
    /// call of <paramref name="accumulator"/>. The output completes once <paramref name="source"/> has
    /// completed and every inner observable has also completed.
    /// </summary>
    /// <remarks>
    /// Like <see cref="MergeMapOperator"/>, inner observables are merged with unbounded concurrency: more than
    /// one accumulator-produced observable can be active at the same time, so the retained state is updated in
    /// whatever order inner values actually arrive rather than in source-emission order.
    /// If <paramref name="accumulator"/> throws, the exception is forwarded to the subscriber via <c>OnError</c>
    /// instead of propagating synchronously. An error from <paramref name="source"/> or from any inner
    /// observable also terminates the output immediately via <c>OnError</c>.
    /// </remarks>
    /// <typeparam name="TSource">The type of values emitted by <paramref name="source"/>.</typeparam>
    /// <typeparam name="TAcc">The type of the accumulated state, of the values emitted by the inner observables, and of the output.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="accumulator">
    /// A function called with the current accumulated state and the source value; returns an inner observable
    /// whose values become the new accumulated state.
    /// </param>
    /// <param name="seed">The initial accumulated state, used for the first call to <paramref name="accumulator"/>.</param>
    /// <returns>An observable of the values produced by the accumulator's inner observables.</returns>
    public static Observable<TAcc> MergeScan<TSource, TAcc>(this Observable<TSource> source, Func<TAcc, TSource, Observable<TAcc>> accumulator, TAcc seed)
        => source.MergeScan((acc, value, _) => accumulator(acc, value), seed);
}
