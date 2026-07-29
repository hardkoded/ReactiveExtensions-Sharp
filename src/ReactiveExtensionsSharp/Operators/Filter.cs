namespace ReactiveExtensionsSharp.Operators;

/// <summary>Implements the <c>Filter</c> operator. Mirrors rxjs's <c>filter</c>.</summary>
public static class FilterOperator
{
    /// <summary>
    /// Emits only the values from <paramref name="source"/> for which <paramref name="predicate"/> returns <see langword="true"/>.
    /// </summary>
    /// <remarks>Equivalent to the indexed overload with the index ignored. If <paramref name="predicate"/> throws, the exception is forwarded via <c>OnError</c>.</remarks>
    /// <typeparam name="T">The element type of the source observable.</typeparam>
    /// <param name="source">The source observable to filter.</param>
    /// <param name="predicate">A function that tests each value.</param>
    /// <returns>An observable of the values from <paramref name="source"/> that satisfy <paramref name="predicate"/>.</returns>
    public static Observable<T> Filter<T>(this Observable<T> source, Func<T, bool> predicate)
        => source.Filter((value, _) => predicate(value));

    /// <summary>
    /// Emits only the values from <paramref name="source"/> for which <paramref name="predicate"/>, called with
    /// the value and its zero-based emission index, returns <see langword="true"/>.
    /// </summary>
    /// <remarks>If <paramref name="predicate"/> throws, the exception is forwarded via <c>OnError</c> and the source subscription is torn down.</remarks>
    /// <typeparam name="T">The element type of the source observable.</typeparam>
    /// <param name="source">The source observable to filter.</param>
    /// <param name="predicate">A function that tests each value together with its index since subscription.</param>
    /// <returns>An observable of the values from <paramref name="source"/> that satisfy <paramref name="predicate"/>.</returns>
    public static Observable<T> Filter<T>(this Observable<T> source, Func<T, int, bool> predicate)
        => source.Operate<T, T>((src, subscriber) =>
        {
            var index = 0;
            return src.SubscribeChild(
                subscriber,
                onNext: value =>
                {
                    bool matches;
                    try
                    {
                        matches = predicate(value, index++);
                    }
                    catch (Exception ex)
                    {
                        subscriber.OnError(ex);
                        return;
                    }

                    if (matches)
                    {
                        subscriber.OnNext(value);
                    }
                },
                onError: subscriber.OnError,
                onComplete: subscriber.OnCompleted);
        });
}
