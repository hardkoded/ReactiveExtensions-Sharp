namespace RxSharp.Operators;

/// <summary>Implements the <c>First</c> operator. Mirrors rxjs's <c>first</c>.</summary>
public static class FirstOperator
{
    /// <summary>
    /// Emits only the first value from <paramref name="source"/>, then completes immediately without waiting for
    /// <paramref name="source"/> itself to complete.
    /// </summary>
    /// <remarks>
    /// If <paramref name="source"/> completes without emitting any value, the result errors with an
    /// <see cref="EmptyErrorException"/> instead of completing &#8212; this is what distinguishes <c>First()</c>
    /// from <c>Take(1)</c>.
    /// </remarks>
    /// <typeparam name="T">The element type of the source observable.</typeparam>
    /// <param name="source">The source observable to take the first value from.</param>
    /// <returns>An observable of just the first value from <paramref name="source"/>.</returns>
    public static Observable<T> First<T>(this Observable<T> source)
        => source.FirstCore((_, _) => true, hasDefault: false, default!);

    /// <summary>
    /// Emits only the first value from <paramref name="source"/>, or <paramref name="defaultValue"/> if it
    /// completes without emitting any value.
    /// </summary>
    /// <typeparam name="T">The element type of the source observable and the default value.</typeparam>
    /// <param name="source">The source observable to take the first value from.</param>
    /// <param name="defaultValue">The value to emit if <paramref name="source"/> completes without emitting anything.</param>
    /// <returns>An observable of the first value from <paramref name="source"/>, or <paramref name="defaultValue"/> if it is empty.</returns>
    public static Observable<T> First<T>(this Observable<T> source, T defaultValue)
        => source.FirstCore((_, _) => true, hasDefault: true, defaultValue);

    /// <summary>
    /// Emits only the first value from <paramref name="source"/> that satisfies <paramref name="predicate"/>, then
    /// completes.
    /// </summary>
    /// <remarks>
    /// If no value satisfies <paramref name="predicate"/> before <paramref name="source"/> completes, the result
    /// errors with an <see cref="EmptyErrorException"/>. If <paramref name="predicate"/> throws, that exception is
    /// forwarded via <c>OnError</c> instead.
    /// </remarks>
    /// <typeparam name="T">The element type of the source observable.</typeparam>
    /// <param name="source">The source observable to search.</param>
    /// <param name="predicate">A function that tests each value.</param>
    /// <returns>An observable of the first value from <paramref name="source"/> that satisfies <paramref name="predicate"/>.</returns>
    public static Observable<T> First<T>(this Observable<T> source, Func<T, bool> predicate)
        => source.FirstCore((value, _) => predicate(value), hasDefault: false, default!);

    /// <summary>
    /// Emits only the first value from <paramref name="source"/> that satisfies <paramref name="predicate"/>, or
    /// <paramref name="defaultValue"/> if none does before <paramref name="source"/> completes.
    /// </summary>
    /// <remarks>If <paramref name="predicate"/> throws, the exception is forwarded via <c>OnError</c>.</remarks>
    /// <typeparam name="T">The element type of the source observable and the default value.</typeparam>
    /// <param name="source">The source observable to search.</param>
    /// <param name="predicate">A function that tests each value.</param>
    /// <param name="defaultValue">The value to emit if no value satisfies <paramref name="predicate"/>.</param>
    /// <returns>An observable of the first matching value, or <paramref name="defaultValue"/> if none is found.</returns>
    public static Observable<T> First<T>(this Observable<T> source, Func<T, bool> predicate, T defaultValue)
        => source.FirstCore((value, _) => predicate(value), hasDefault: true, defaultValue);

    /// <summary>
    /// Emits only the first value from <paramref name="source"/> that satisfies <paramref name="predicate"/>
    /// (called with the value and its zero-based emission index), then completes.
    /// </summary>
    /// <remarks>
    /// If no value satisfies <paramref name="predicate"/> before <paramref name="source"/> completes, the result
    /// errors with an <see cref="EmptyErrorException"/>. If <paramref name="predicate"/> throws, that exception is
    /// forwarded via <c>OnError</c> instead.
    /// </remarks>
    /// <typeparam name="T">The element type of the source observable.</typeparam>
    /// <param name="source">The source observable to search.</param>
    /// <param name="predicate">A function that tests each value together with its index since subscription.</param>
    /// <returns>An observable of the first value from <paramref name="source"/> that satisfies <paramref name="predicate"/>.</returns>
    public static Observable<T> First<T>(this Observable<T> source, Func<T, int, bool> predicate)
        => source.FirstCore(predicate, hasDefault: false, default!);

    /// <summary>
    /// Emits only the first value from <paramref name="source"/> that satisfies <paramref name="predicate"/>
    /// (called with the value and its zero-based emission index), or <paramref name="defaultValue"/> if none does
    /// before <paramref name="source"/> completes.
    /// </summary>
    /// <remarks>If <paramref name="predicate"/> throws, the exception is forwarded via <c>OnError</c>.</remarks>
    /// <typeparam name="T">The element type of the source observable and the default value.</typeparam>
    /// <param name="source">The source observable to search.</param>
    /// <param name="predicate">A function that tests each value together with its index since subscription.</param>
    /// <param name="defaultValue">The value to emit if no value satisfies <paramref name="predicate"/>.</param>
    /// <returns>An observable of the first matching value, or <paramref name="defaultValue"/> if none is found.</returns>
    public static Observable<T> First<T>(this Observable<T> source, Func<T, int, bool> predicate, T defaultValue)
        => source.FirstCore(predicate, hasDefault: true, defaultValue);

    private static Observable<T> FirstCore<T>(this Observable<T> source, Func<T, int, bool> predicate, bool hasDefault, T defaultValue)
        => source.Operate<T, T>((src, subscriber) =>
        {
            var index = 0;
            var sourceSubscription = new SingleAssignmentDisposable();
            sourceSubscription.Disposable = src.Subscribe(
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
                        subscriber.OnCompleted();
                        sourceSubscription.Dispose();
                    }
                },
                onError: subscriber.OnError,
                onComplete: () =>
                {
                    if (hasDefault)
                    {
                        subscriber.OnNext(defaultValue);
                        subscriber.OnCompleted();
                    }
                    else
                    {
                        subscriber.OnError(new EmptyErrorException());
                    }
                });

            return sourceSubscription;
        });
}
