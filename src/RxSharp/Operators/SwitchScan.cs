namespace RxSharp.Operators;

/// <summary>Extension methods implementing the <c>switchScan</c> operator.</summary>
public static class SwitchScanOperator
{
    /// <summary>
    /// Applies <paramref name="accumulator"/> to each value emitted by <paramref name="source"/>, along with its
    /// zero-based index, subscribing to the inner observable it returns and mirroring only the most recently
    /// returned one — like <see cref="SwitchMapOperator"/>, the previous inner observable's subscription is torn
    /// down (even if still live) before subscribing to the new one. The last value produced by an inner
    /// observable becomes the new accumulated state, passed as <c>acc</c> to the next call of
    /// <paramref name="accumulator"/>. The output completes once <paramref name="source"/> has completed and the
    /// current inner observable, if any, has also completed.
    /// </summary>
    /// <remarks>
    /// This is <see cref="MergeScanOperator"/> combined with <see cref="SwitchMapOperator"/>'s switch-instead-of-merge
    /// behavior. If <paramref name="accumulator"/> throws, the exception is forwarded to the subscriber via
    /// <c>OnError</c> instead of propagating synchronously. An error from <paramref name="source"/> or from the
    /// active inner observable also terminates the output immediately via <c>OnError</c>.
    /// </remarks>
    /// <typeparam name="TSource">The type of values emitted by <paramref name="source"/>.</typeparam>
    /// <typeparam name="TAcc">The type of the accumulated state, of the values emitted by the inner observables, and of the output.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="accumulator">
    /// A function called with the current accumulated state, the source value, and its index (starting at 0);
    /// returns an inner observable to switch to, whose values become the new accumulated state.
    /// </param>
    /// <param name="seed">The initial accumulated state, used for the first call to <paramref name="accumulator"/>.</param>
    /// <returns>An observable of the values produced by only the most recently switched-to inner observable.</returns>
    public static Observable<TAcc> SwitchScan<TSource, TAcc>(this Observable<TSource> source, Func<TAcc, TSource, int, Observable<TAcc>> accumulator, TAcc seed)
        => source.Operate<TSource, TAcc>((src, subscriber) =>
        {
            var state = seed;
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

                    // SingleAssignmentDisposable only overwrites; switchScan needs the previous (still-live)
                    // inner subscription actually torn down before starting the new one — same reasoning as
                    // SwitchMap.
                    innerSubscription.Disposable?.Dispose();
                    hasInner = true;

                    // Built directly and assigned into the (already-registered-with-`subscriber`) slot BEFORE
                    // `inner.Subscribe` runs, so a fully-synchronous inner is stopped mid-loop by a downstream
                    // disposal instead of only after Subscribe returns.
                    Subscriber<TAcc>? newInnerSubscriber = null;
                    newInnerSubscriber = Subscriber.Create<TAcc>(
                        onNext: result =>
                        {
                            state = result;
                            subscriber.OnNext(result);
                        },
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

    /// <summary>
    /// Applies <paramref name="accumulator"/> to each value emitted by <paramref name="source"/>, subscribing to
    /// the inner observable it returns and mirroring only the most recently returned one. The last value
    /// produced by an inner observable becomes the new accumulated state, passed as <c>acc</c> to the next call
    /// of <paramref name="accumulator"/>.
    /// </summary>
    /// <typeparam name="TSource">The type of values emitted by <paramref name="source"/>.</typeparam>
    /// <typeparam name="TAcc">The type of the accumulated state, of the values emitted by the inner observables, and of the output.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="accumulator">
    /// A function called with the current accumulated state and the source value; returns an inner observable to
    /// switch to, whose values become the new accumulated state.
    /// </param>
    /// <param name="seed">The initial accumulated state, used for the first call to <paramref name="accumulator"/>.</param>
    /// <returns>An observable of the values produced by only the most recently switched-to inner observable.</returns>
    public static Observable<TAcc> SwitchScan<TSource, TAcc>(this Observable<TSource> source, Func<TAcc, TSource, Observable<TAcc>> accumulator, TAcc seed)
        => source.SwitchScan((acc, value, _) => accumulator(acc, value), seed);
}
