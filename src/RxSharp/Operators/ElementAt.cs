namespace RxSharp.Operators;

/// <summary>Implements the <c>ElementAt</c> operator. Mirrors rxjs's <c>elementAt</c>.</summary>
public static class ElementAtOperator
{
    /// <summary>
    /// Emits only the value at the given zero-based <paramref name="index"/> of <paramref name="source"/>, then
    /// completes without waiting for <paramref name="source"/> itself to complete.
    /// </summary>
    /// <remarks>
    /// If <paramref name="source"/> completes before reaching <paramref name="index"/>, the result errors with an
    /// <see cref="ArgumentOutOfRangeException"/> instead of completing.
    /// </remarks>
    /// <typeparam name="T">The element type of the source observable.</typeparam>
    /// <param name="source">The source observable to index into.</param>
    /// <param name="index">The zero-based index of the value to emit.</param>
    /// <returns>An observable of just the value at <paramref name="index"/>.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="index"/> is negative.</exception>
    public static Observable<T> ElementAt<T>(this Observable<T> source, int index)
    {
        ThrowIfNegative(index);
        return source.ElementAtCore(index, hasDefault: false, default!);
    }

    /// <summary>
    /// Emits only the value at the given zero-based <paramref name="index"/> of <paramref name="source"/>, or
    /// <paramref name="defaultValue"/> if <paramref name="source"/> completes before reaching that index.
    /// </summary>
    /// <typeparam name="T">The element type of the source observable and the default value.</typeparam>
    /// <param name="source">The source observable to index into.</param>
    /// <param name="index">The zero-based index of the value to emit.</param>
    /// <param name="defaultValue">The value to emit if <paramref name="source"/> completes before reaching <paramref name="index"/>.</param>
    /// <returns>An observable of the value at <paramref name="index"/>, or <paramref name="defaultValue"/> if out of range.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="index"/> is negative.</exception>
    public static Observable<T> ElementAt<T>(this Observable<T> source, int index, T defaultValue)
    {
        ThrowIfNegative(index);
        return source.ElementAtCore(index, hasDefault: true, defaultValue);
    }

    private static Observable<T> ElementAtCore<T>(this Observable<T> source, int index, bool hasDefault, T defaultValue)
        => source.Operate<T, T>((src, subscriber) =>
        {
            var current = 0;

            // Built directly (see Take.cs for the full explanation) so this closure can dispose the inner
            // subscriber immediately, even from within a nested/synchronous source callback.
            Subscriber<T> innerSubscriber = null!;
            innerSubscriber = Subscriber.Create<T>(
                onNext: value =>
                {
                    if (current++ == index)
                    {
                        subscriber.OnNext(value);
                        subscriber.OnCompleted();
                        innerSubscriber.Dispose();
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
                        subscriber.OnError(new ArgumentOutOfRangeException(nameof(index)));
                    }
                });

            return src.Subscribe(innerSubscriber);
        });

    private static void ThrowIfNegative(int index)
    {
#if NET8_0_OR_GREATER
        ArgumentOutOfRangeException.ThrowIfNegative(index);
#else
        if (index < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(index));
        }
#endif
    }
}
