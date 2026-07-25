namespace RxSharp.Operators;

/// <summary>Extension methods implementing the <c>map</c> operator.</summary>
public static class MapOperator
{
    /// <summary>
    /// Applies <paramref name="project"/> to each value emitted by <paramref name="source"/> and emits the
    /// resulting value in its place. Errors and completion are passed through unchanged.
    /// </summary>
    /// <remarks>
    /// If <paramref name="project"/> throws, the exception is forwarded to the subscriber via <c>OnError</c>
    /// instead of propagating synchronously out of <c>Subscribe</c>.
    /// </remarks>
    /// <typeparam name="TSource">The type of values emitted by <paramref name="source"/>.</typeparam>
    /// <typeparam name="TResult">The type of values produced by <paramref name="project"/>.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="project">A transform function applied to each source value.</param>
    /// <returns>An observable of the projected values.</returns>
    public static Observable<TResult> Map<TSource, TResult>(this Observable<TSource> source, Func<TSource, TResult> project)
        => source.Map((value, _) => project(value));

    /// <summary>
    /// Applies <paramref name="project"/> to each value emitted by <paramref name="source"/>, along with the
    /// zero-based index of that emission, and emits the resulting value in its place. Errors and completion
    /// are passed through unchanged.
    /// </summary>
    /// <remarks>
    /// If <paramref name="project"/> throws, the exception is forwarded to the subscriber via <c>OnError</c>
    /// instead of propagating synchronously out of <c>Subscribe</c>.
    /// </remarks>
    /// <typeparam name="TSource">The type of values emitted by <paramref name="source"/>.</typeparam>
    /// <typeparam name="TResult">The type of values produced by <paramref name="project"/>.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="project">A transform function applied to each source value and its index (starting at 0).</param>
    /// <returns>An observable of the projected values.</returns>
    public static Observable<TResult> Map<TSource, TResult>(this Observable<TSource> source, Func<TSource, int, TResult> project)
        => source.Operate<TSource, TResult>((src, subscriber) =>
        {
            var index = 0;
            return src.Subscribe(
                onNext: value =>
                {
                    TResult result;
                    try
                    {
                        result = project(value, index++);
                    }
                    catch (Exception ex)
                    {
                        subscriber.OnError(ex);
                        return;
                    }

                    subscriber.OnNext(result);
                },
                onError: subscriber.OnError,
                onComplete: subscriber.OnCompleted);
        });
}
