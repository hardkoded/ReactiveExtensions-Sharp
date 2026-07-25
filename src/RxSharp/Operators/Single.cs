namespace RxSharp.Operators;

/// <summary>Implements the <c>Single</c> operator. Mirrors rxjs's <c>single</c>.</summary>
public static class SingleOperator
{
    /// <summary>
    /// Asserts that <paramref name="source"/> emits exactly one value, and emits that value once
    /// <paramref name="source"/> completes.
    /// </summary>
    /// <remarks>
    /// If <paramref name="source"/> completes without ever emitting a value, the result errors with an
    /// <see cref="EmptyErrorException"/>. If <paramref name="source"/> emits more than one value, the result
    /// errors with a <see cref="SequenceErrorException"/> as soon as the second value arrives.
    /// </remarks>
    /// <typeparam name="T">The element type of the source observable.</typeparam>
    /// <param name="source">The source observable to assert singularity of.</param>
    /// <returns>An observable of the single value emitted by <paramref name="source"/>.</returns>
    public static Observable<T> Single<T>(this Observable<T> source)
        => source.SingleCore((_, _) => true);

    /// <summary>
    /// Asserts that exactly one value emitted by <paramref name="source"/> satisfies <paramref name="predicate"/>,
    /// and emits that value once <paramref name="source"/> completes.
    /// </summary>
    /// <remarks>
    /// If <paramref name="source"/> completes without emitting any value at all, the result errors with an
    /// <see cref="EmptyErrorException"/>. If <paramref name="source"/> emits one or more values but none of them
    /// satisfy <paramref name="predicate"/>, the result errors with a <see cref="NotFoundErrorException"/>. If more
    /// than one value satisfies <paramref name="predicate"/>, the result errors with a
    /// <see cref="SequenceErrorException"/> as soon as the second match arrives. If <paramref name="predicate"/>
    /// throws, that exception is forwarded via <c>OnError</c> instead.
    /// </remarks>
    /// <typeparam name="T">The element type of the source observable.</typeparam>
    /// <param name="source">The source observable to search.</param>
    /// <param name="predicate">A function that tests each value.</param>
    /// <returns>An observable of the single matching value.</returns>
    public static Observable<T> Single<T>(this Observable<T> source, Func<T, bool> predicate)
        => source.SingleCore((value, _) => predicate(value));

    /// <summary>
    /// Asserts that exactly one value emitted by <paramref name="source"/> satisfies <paramref name="predicate"/>
    /// (called with the value and its zero-based emission index), and emits that value once
    /// <paramref name="source"/> completes.
    /// </summary>
    /// <remarks>
    /// If <paramref name="source"/> completes without emitting any value at all, the result errors with an
    /// <see cref="EmptyErrorException"/>. If <paramref name="source"/> emits one or more values but none of them
    /// satisfy <paramref name="predicate"/>, the result errors with a <see cref="NotFoundErrorException"/>. If more
    /// than one value satisfies <paramref name="predicate"/>, the result errors with a
    /// <see cref="SequenceErrorException"/> as soon as the second match arrives. If <paramref name="predicate"/>
    /// throws, that exception is forwarded via <c>OnError</c> instead.
    /// </remarks>
    /// <typeparam name="T">The element type of the source observable.</typeparam>
    /// <param name="source">The source observable to search.</param>
    /// <param name="predicate">A function that tests each value together with its index since subscription.</param>
    /// <returns>An observable of the single matching value.</returns>
    public static Observable<T> Single<T>(this Observable<T> source, Func<T, int, bool> predicate)
        => source.SingleCore(predicate);

    private static Observable<T> SingleCore<T>(this Observable<T> source, Func<T, int, bool> predicate)
        => source.Operate<T, T>((src, subscriber) =>
        {
            var index = 0;
            var hasValue = false;
            var seenValue = false;
            T singleValue = default!;

            // Built directly (see Take.cs for the full explanation) so this closure can dispose the inner
            // subscriber immediately as soon as a second match is found, even from within a nested/synchronous
            // source callback.
            Subscriber<T> innerSubscriber = null!;
            innerSubscriber = Subscriber.Create<T>(
                onNext: value =>
                {
                    seenValue = true;

                    bool matches;
                    try
                    {
                        matches = predicate(value, index++);
                    }
                    catch (Exception ex)
                    {
                        subscriber.OnError(ex);
                        innerSubscriber.Dispose();
                        return;
                    }

                    if (matches)
                    {
                        if (hasValue)
                        {
                            subscriber.OnError(new SequenceErrorException());
                            innerSubscriber.Dispose();
                            return;
                        }

                        hasValue = true;
                        singleValue = value;
                    }
                },
                onError: subscriber.OnError,
                onComplete: () =>
                {
                    if (hasValue)
                    {
                        subscriber.OnNext(singleValue);
                        subscriber.OnCompleted();
                    }
                    else
                    {
                        subscriber.OnError(seenValue ? new NotFoundErrorException() : new EmptyErrorException());
                    }
                });

            return src.Subscribe(innerSubscriber);
        });
}
