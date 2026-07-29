namespace ReactiveExtensionsSharp.Operators;

/// <summary>Implements the <c>Last</c> operator. Mirrors rxjs's <c>last</c>.</summary>
public static class LastOperator
{
    /// <summary>
    /// Emits only the last value from <paramref name="source"/>, once it completes.
    /// </summary>
    /// <remarks>
    /// If <paramref name="source"/> completes without emitting any value, the result errors with an
    /// <see cref="EmptyErrorException"/> instead of completing &#8212; this is what distinguishes <c>Last()</c>
    /// from <c>TakeLast(1)</c>.
    /// </remarks>
    /// <typeparam name="T">The element type of the source observable.</typeparam>
    /// <param name="source">The source observable to take the last value from.</param>
    /// <returns>An observable of just the last value from <paramref name="source"/>.</returns>
    public static Observable<T> Last<T>(this Observable<T> source)
        => source.LastCore((_, _) => true, hasDefault: false, default!);

    /// <summary>
    /// Emits only the last value from <paramref name="source"/>, or <paramref name="defaultValue"/> if it
    /// completes without emitting any value.
    /// </summary>
    /// <typeparam name="T">The element type of the source observable and the default value.</typeparam>
    /// <param name="source">The source observable to take the last value from.</param>
    /// <param name="defaultValue">The value to emit if <paramref name="source"/> completes without emitting anything.</param>
    /// <returns>An observable of the last value from <paramref name="source"/>, or <paramref name="defaultValue"/> if it is empty.</returns>
    public static Observable<T> Last<T>(this Observable<T> source, T defaultValue)
        => source.LastCore((_, _) => true, hasDefault: true, defaultValue);

    /// <summary>
    /// Emits only the last value from <paramref name="source"/> that satisfies <paramref name="predicate"/>, once
    /// <paramref name="source"/> completes.
    /// </summary>
    /// <remarks>
    /// If no value satisfies <paramref name="predicate"/>, the result errors with an <see cref="EmptyErrorException"/>.
    /// If <paramref name="predicate"/> throws, that exception is forwarded via <c>OnError</c> instead.
    /// </remarks>
    /// <typeparam name="T">The element type of the source observable.</typeparam>
    /// <param name="source">The source observable to search.</param>
    /// <param name="predicate">A function that tests each value.</param>
    /// <returns>An observable of the last value from <paramref name="source"/> that satisfies <paramref name="predicate"/>.</returns>
    public static Observable<T> Last<T>(this Observable<T> source, Func<T, bool> predicate)
        => source.LastCore((value, _) => predicate(value), hasDefault: false, default!);

    /// <summary>
    /// Emits only the last value from <paramref name="source"/> that satisfies <paramref name="predicate"/>, or
    /// <paramref name="defaultValue"/> if none does before <paramref name="source"/> completes.
    /// </summary>
    /// <remarks>If <paramref name="predicate"/> throws, the exception is forwarded via <c>OnError</c>.</remarks>
    /// <typeparam name="T">The element type of the source observable and the default value.</typeparam>
    /// <param name="source">The source observable to search.</param>
    /// <param name="predicate">A function that tests each value.</param>
    /// <param name="defaultValue">The value to emit if no value satisfies <paramref name="predicate"/>.</param>
    /// <returns>An observable of the last matching value, or <paramref name="defaultValue"/> if none is found.</returns>
    public static Observable<T> Last<T>(this Observable<T> source, Func<T, bool> predicate, T defaultValue)
        => source.LastCore((value, _) => predicate(value), hasDefault: true, defaultValue);

    /// <summary>
    /// Emits only the last value from <paramref name="source"/> that satisfies <paramref name="predicate"/>
    /// (called with the value and its zero-based emission index), once <paramref name="source"/> completes.
    /// </summary>
    /// <remarks>
    /// If no value satisfies <paramref name="predicate"/>, the result errors with an <see cref="EmptyErrorException"/>.
    /// If <paramref name="predicate"/> throws, that exception is forwarded via <c>OnError</c> instead.
    /// </remarks>
    /// <typeparam name="T">The element type of the source observable.</typeparam>
    /// <param name="source">The source observable to search.</param>
    /// <param name="predicate">A function that tests each value together with its index since subscription.</param>
    /// <returns>An observable of the last value from <paramref name="source"/> that satisfies <paramref name="predicate"/>.</returns>
    public static Observable<T> Last<T>(this Observable<T> source, Func<T, int, bool> predicate)
        => source.LastCore(predicate, hasDefault: false, default!);

    /// <summary>
    /// Emits only the last value from <paramref name="source"/> that satisfies <paramref name="predicate"/>
    /// (called with the value and its zero-based emission index), or <paramref name="defaultValue"/> if none
    /// does before <paramref name="source"/> completes.
    /// </summary>
    /// <remarks>If <paramref name="predicate"/> throws, the exception is forwarded via <c>OnError</c>.</remarks>
    /// <typeparam name="T">The element type of the source observable and the default value.</typeparam>
    /// <param name="source">The source observable to search.</param>
    /// <param name="predicate">A function that tests each value together with its index since subscription.</param>
    /// <param name="defaultValue">The value to emit if no value satisfies <paramref name="predicate"/>.</param>
    /// <returns>An observable of the last matching value, or <paramref name="defaultValue"/> if none is found.</returns>
    public static Observable<T> Last<T>(this Observable<T> source, Func<T, int, bool> predicate, T defaultValue)
        => source.LastCore(predicate, hasDefault: true, defaultValue);

    private static Observable<T> LastCore<T>(this Observable<T> source, Func<T, int, bool> predicate, bool hasDefault, T defaultValue)
        => source.Operate<T, T>((src, subscriber) =>
        {
            var index = 0;
            var hasValue = false;
            T lastValue = default!;

            return src.Subscribe(
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
                        hasValue = true;
                        lastValue = value;
                    }
                },
                onError: subscriber.OnError,
                onComplete: () =>
                {
                    if (hasValue)
                    {
                        subscriber.OnNext(lastValue);
                        subscriber.OnCompleted();
                    }
                    else if (hasDefault)
                    {
                        subscriber.OnNext(defaultValue);
                        subscriber.OnCompleted();
                    }
                    else
                    {
                        subscriber.OnError(new EmptyErrorException());
                    }
                });
        });
}
