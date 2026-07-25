namespace RxSharp.Operators;

/// <summary>Implements the <c>SkipWhile</c> operator. Mirrors rxjs's <c>skipWhile</c>.</summary>
public static class SkipWhileOperator
{
    /// <summary>
    /// Skips values from <paramref name="source"/> for as long as <paramref name="predicate"/> returns
    /// <see langword="true"/>, then emits that value and every value after it, unchanged.
    /// </summary>
    /// <remarks>Equivalent to the indexed overload with the index ignored. If <paramref name="predicate"/> throws, the exception is forwarded via <c>OnError</c>.</remarks>
    /// <typeparam name="T">The element type of the source observable.</typeparam>
    /// <param name="source">The source observable to skip values from.</param>
    /// <param name="predicate">A function that tests each value while skipping is still in effect.</param>
    /// <returns>An observable that starts emitting once <paramref name="predicate"/> first returns <see langword="false"/>.</returns>
    public static Observable<T> SkipWhile<T>(this Observable<T> source, Func<T, bool> predicate)
        => source.SkipWhile((value, _) => predicate(value));

    /// <summary>
    /// Skips values from <paramref name="source"/> for as long as <paramref name="predicate"/>, called with the
    /// value and its zero-based emission index, returns <see langword="true"/>, then emits that value and every
    /// value after it, unchanged.
    /// </summary>
    /// <remarks>
    /// Once <paramref name="predicate"/> has returned <see langword="false"/> for the first time, it is never
    /// invoked again for the rest of the subscription &#8212; this is a "sticky" transition, matching rxjs's
    /// <c>skipWhile</c> exactly. If <paramref name="predicate"/> throws, the exception is forwarded via <c>OnError</c>.
    /// </remarks>
    /// <typeparam name="T">The element type of the source observable.</typeparam>
    /// <param name="source">The source observable to skip values from.</param>
    /// <param name="predicate">A function that tests each value together with its index, while skipping is still in effect.</param>
    /// <returns>An observable that starts emitting once <paramref name="predicate"/> first returns <see langword="false"/>.</returns>
    public static Observable<T> SkipWhile<T>(this Observable<T> source, Func<T, int, bool> predicate)
        => source.Operate<T, T>((src, subscriber) =>
        {
            var taking = false;
            var index = 0;

            // Built directly (see Take.cs / Skip.cs for the full explanation) so this closure can cascade a
            // downstream early-completion (e.g. a Take further down the chain) into disposing our own
            // subscription to src immediately, even for a synchronous, self-checking source.
            Subscriber<T> innerSubscriber = null!;
            innerSubscriber = Subscriber.Create<T>(
                onNext: value =>
                {
                    if (!taking)
                    {
                        bool result;
                        try
                        {
                            result = predicate(value, index++);
                        }
                        catch (Exception ex)
                        {
                            subscriber.OnError(ex);
                            innerSubscriber.Dispose();
                            return;
                        }

                        taking = !result;
                    }

                    if (taking)
                    {
                        subscriber.OnNext(value);
                    }

                    if (subscriber.IsDisposed)
                    {
                        innerSubscriber.Dispose();
                    }
                },
                onError: subscriber.OnError,
                onComplete: subscriber.OnCompleted);

            return src.Subscribe(innerSubscriber);
        });
}
