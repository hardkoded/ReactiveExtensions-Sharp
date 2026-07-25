namespace RxSharp.Operators;

/// <summary>Implements the <c>ConcatMap</c> operator. Mirrors rxjs's <c>concatMap</c>.</summary>
public static class ConcatMapOperator
{
    /// <summary>
    /// Projects each source value into an inner observable via <paramref name="project"/>, and concatenates the
    /// resulting inner observables in order: the next inner observable is only subscribed to once the previous
    /// one has completed, so their emitted values are never interleaved.
    /// </summary>
    /// <remarks>Equivalent to the indexed overload with the index ignored.</remarks>
    /// <typeparam name="TSource">The element type of the source observable.</typeparam>
    /// <typeparam name="TResult">The element type produced by the inner observables.</typeparam>
    /// <param name="source">The source observable whose values are projected.</param>
    /// <param name="project">A function that maps each source value to an inner observable.</param>
    /// <returns>An observable of the concatenated values from each inner observable, in source order.</returns>
    public static Observable<TResult> ConcatMap<TSource, TResult>(this Observable<TSource> source, Func<TSource, Observable<TResult>> project)
        => source.ConcatMap((value, _) => project(value));

    /// <summary>
    /// Projects each source value (together with its zero-based processing index) into an inner observable via
    /// <paramref name="project"/>, and subscribes to the inner observables one at a time, in the order their
    /// source values arrived.
    /// </summary>
    /// <remarks>
    /// Source values that arrive while an inner observable is still active are queued rather than dropped: the
    /// next queued value is only projected and subscribed to once the current inner observable completes,
    /// preserving strict FIFO order. If <paramref name="project"/> throws, or an inner observable errors, the
    /// error is forwarded via <c>OnError</c> and no further inner observables are subscribed to. Completion is
    /// forwarded only once <paramref name="source"/> has completed and every queued inner observable has
    /// completed.
    /// </remarks>
    /// <typeparam name="TSource">The element type of the source observable.</typeparam>
    /// <typeparam name="TResult">The element type produced by the inner observables.</typeparam>
    /// <param name="source">The source observable whose values are projected.</param>
    /// <param name="project">A function that maps each source value and its processing index to an inner observable.</param>
    /// <returns>An observable of the concatenated values from each inner observable, in source order.</returns>
    public static Observable<TResult> ConcatMap<TSource, TResult>(this Observable<TSource> source, Func<TSource, int, Observable<TResult>> project)
        => source.Operate<TSource, TResult>((src, subscriber) =>
        {
            var index = 0;
            var queue = new Queue<TSource>();
            var isSourceComplete = false;
            var isInnerActive = false;

            void SubscribeNext()
            {
                if (isInnerActive)
                {
                    return;
                }

                if (queue.Count == 0)
                {
                    if (isSourceComplete)
                    {
                        subscriber.OnCompleted();
                    }

                    return;
                }

                var value = queue.Dequeue();
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

                isInnerActive = true;
                var innerSubscription = new SingleAssignmentDisposable();
                innerSubscription.Disposable = inner.Subscribe(
                    onNext: subscriber.OnNext,
                    onError: subscriber.OnError,
                    onComplete: () =>
                    {
                        isInnerActive = false;
                        SubscribeNext();
                    });
                subscriber.Add(innerSubscription);
            }

            return src.Subscribe(
                onNext: value =>
                {
                    queue.Enqueue(value);
                    SubscribeNext();
                },
                onError: subscriber.OnError,
                onComplete: () =>
                {
                    isSourceComplete = true;
                    SubscribeNext();
                });
        });
}
