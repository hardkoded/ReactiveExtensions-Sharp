namespace RxSharp.Operators;

/// <summary>
/// Extension methods implementing the <c>expand</c> operator.
/// </summary>
/// <remarks>
/// Like <see cref="MergeMapOperator"/>, this port always merges with unbounded concurrency: there is no
/// <c>concurrent</c> limit parameter, since Puppeteer's own usage (the motivating consumer of this port) never
/// needs to cap in-flight inner subscriptions. Unlike <c>MergeMap</c>, an unbounded concurrency limit is arguably
/// more important to flag here: a <c>project</c> that never returns an empty/completing-without-emitting
/// observable recurses forever, and — since C# gives no bottom type — <c>project</c>'s return type must be
/// <c>Observable&lt;T&gt;</c> (the same as the source), so it is easy to write an accidentally-infinite expansion.
/// Rely on a downstream operator like <c>Take</c> to cut it off, exactly as rxjs's own documentation examples do.
/// </remarks>
public static class ExpandOperator
{
    /// <summary>
    /// Recursively projects each value — from <paramref name="source"/> itself, and then from every inner
    /// observable <paramref name="project"/> produces — back through <paramref name="project"/>, merging every
    /// resulting inner observable into the output.
    /// </summary>
    /// <remarks>See the indexed overload for the full behavior description.</remarks>
    /// <typeparam name="T">The type of values emitted by <paramref name="source"/>, by every inner observable, and by the output.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="project">A function that maps each value (from the source or from a previous expansion) to an inner observable to expand further.</param>
    /// <returns>An observable that emits every value encountered while recursively expanding <paramref name="source"/>.</returns>
    public static Observable<T> Expand<T>(this Observable<T> source, Func<T, Observable<T>> project)
        => source.Expand((value, _) => project(value));

    /// <summary>
    /// Recursively projects each value — from <paramref name="source"/> itself, and then from every inner
    /// observable <paramref name="project"/> produces — along with its zero-based expansion index, back through
    /// <paramref name="project"/>, merging every resulting inner observable into the output.
    /// </summary>
    /// <remarks>
    /// Every value seen — whether it came directly from <paramref name="source"/> or from a previously projected
    /// inner observable — is immediately forwarded to the output, and is also passed to <paramref name="project"/>
    /// to produce a further inner observable to merge in (which is itself expanded the same way, recursively).
    /// The output completes once <paramref name="source"/> has completed and every inner observable produced,
    /// directly or recursively, has also completed. If <paramref name="project"/> throws, or any inner
    /// observable errors, the exception is forwarded to the subscriber via <c>OnError</c> instead of propagating
    /// synchronously.
    /// </remarks>
    /// <typeparam name="T">The type of values emitted by <paramref name="source"/>, by every inner observable, and by the output.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="project">A function that maps each value and its expansion index (starting at 0) to an inner observable to expand further.</param>
    /// <returns>An observable that emits every value encountered while recursively expanding <paramref name="source"/>.</returns>
    public static Observable<T> Expand<T>(this Observable<T> source, Func<T, int, Observable<T>> project)
        => source.Operate<T, T>((src, subscriber) =>
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

            void DoInnerSub(T value)
            {
                // Every value seen -- from the source or from a previous expansion -- is emitted immediately.
                subscriber.OnNext(value);

                // Guards against unbounded recursion once downstream (e.g. Take) has disposed us: without this
                // check, a project that keeps producing synchronously-emitting inner observables would recurse
                // forever even after the consumer has stopped listening. See CLAUDE.md's Learnings for the
                // general "always check for downstream disposal before recursing" pattern this follows.
                if (subscriber.IsDisposed)
                {
                    return;
                }

                Observable<T> inner;
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
                // `inner.Subscribe` runs -- see MergeMap.cs, this operator's closest structural relative, for
                // the same per-value Add/Remove shape. Removed again once it naturally completes, so
                // `subscriber`'s finalizer list doesn't grow unboundedly across a long-running expansion.
                Subscriber<T>? innerSubscriber = null;
                innerSubscriber = Subscriber.Create<T>(
                    onNext: DoInnerSub,
                    onError: subscriber.OnError,
                    onComplete: () =>
                    {
                        activeCount--;
                        subscriber.Remove(innerSubscriber!);
                        CheckComplete();
                    });
                subscriber.Add(innerSubscriber);
                inner.Subscribe(innerSubscriber);
            }

            return src.SubscribeChild(
                subscriber,
                onNext: DoInnerSub,
                onError: subscriber.OnError,
                onComplete: () =>
                {
                    isSourceComplete = true;
                    CheckComplete();
                });
        });
}
