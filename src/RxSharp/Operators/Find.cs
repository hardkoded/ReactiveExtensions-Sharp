namespace RxSharp.Operators;

/// <summary>Implements the <c>Find</c> operator. Mirrors rxjs's <c>find</c>.</summary>
public static class FindOperator
{
    /// <summary>
    /// Emits only the first value from <paramref name="source"/> that satisfies <paramref name="predicate"/>, then
    /// completes.
    /// </summary>
    /// <remarks>
    /// Unlike <c>First(predicate)</c>, if no value satisfies <paramref name="predicate"/> before <paramref name="source"/>
    /// completes, the result emits <see langword="default"/> and completes normally instead of erroring. If
    /// <paramref name="predicate"/> throws, the exception is forwarded via <c>OnError</c>.
    /// </remarks>
    /// <typeparam name="T">The element type of the source observable.</typeparam>
    /// <param name="source">The source observable to search.</param>
    /// <param name="predicate">A function that tests each value.</param>
    /// <returns>An observable of the first matching value (or <see langword="default"/> if none is found), then completes.</returns>
    public static Observable<T?> Find<T>(this Observable<T> source, Func<T, bool> predicate)
        => source.Find((value, _) => predicate(value));

    /// <summary>
    /// Emits only the first value from <paramref name="source"/> that satisfies <paramref name="predicate"/>
    /// (called with the value and its zero-based emission index), then completes.
    /// </summary>
    /// <remarks>
    /// Unlike <c>First(predicate)</c>, if no value satisfies <paramref name="predicate"/> before <paramref name="source"/>
    /// completes, the result emits <see langword="default"/> and completes normally instead of erroring. If
    /// <paramref name="predicate"/> throws, the exception is forwarded via <c>OnError</c>.
    /// </remarks>
    /// <typeparam name="T">The element type of the source observable.</typeparam>
    /// <param name="source">The source observable to search.</param>
    /// <param name="predicate">A function that tests each value together with its index since subscription.</param>
    /// <returns>An observable of the first matching value (or <see langword="default"/> if none is found), then completes.</returns>
    public static Observable<T?> Find<T>(this Observable<T> source, Func<T, int, bool> predicate)
        => source.Operate<T, T?>((src, subscriber)
            => FindCore.Subscribe(src, subscriber, predicate, onMatch: (value, _) => value, noMatchResult: default));
}

/// <summary>Shared "first match" subscription logic used by <see cref="FindOperator"/> and <see cref="FindIndexOperator"/>. Mirrors rxjs's internal <c>createFind</c> helper.</summary>
internal static class FindCore
{
    /// <summary>
    /// Subscribes to <paramref name="src"/>, forwarding either the first value that satisfies <paramref name="predicate"/>
    /// (transformed via <paramref name="onMatch"/>) or, if <paramref name="src"/> completes without a match,
    /// <paramref name="noMatchResult"/> &#8212; then completing either way.
    /// </summary>
    /// <typeparam name="T">The element type of <paramref name="src"/>.</typeparam>
    /// <typeparam name="TResult">The element type emitted downstream.</typeparam>
    /// <param name="src">The source observable to search.</param>
    /// <param name="subscriber">The downstream subscriber to forward the result to.</param>
    /// <param name="predicate">A function that tests each value together with its index since subscription.</param>
    /// <param name="onMatch">Transforms the matching value and its index into the value emitted downstream.</param>
    /// <param name="noMatchResult">The value emitted downstream if <paramref name="src"/> completes without a match.</param>
    /// <returns>A disposable that unsubscribes from <paramref name="src"/>.</returns>
    public static IDisposable Subscribe<T, TResult>(
        Observable<T> src,
        Subscriber<TResult> subscriber,
        Func<T, int, bool> predicate,
        Func<T, int, TResult> onMatch,
        TResult noMatchResult)
    {
        var index = 0;

        // Built directly (see Take.cs for the full explanation) so this closure can dispose the inner
        // subscriber immediately, even from within a nested/synchronous source callback.
        Subscriber<T> innerSubscriber = null!;
        innerSubscriber = Subscriber.Create<T>(
            onNext: value =>
            {
                var currentIndex = index++;
                bool matches;
                try
                {
                    matches = predicate(value, currentIndex);
                }
                catch (Exception ex)
                {
                    subscriber.OnError(ex);
                    return;
                }

                if (matches)
                {
                    subscriber.OnNext(onMatch(value, currentIndex));
                    subscriber.OnCompleted();
                    innerSubscriber.Dispose();
                }
            },
            onError: subscriber.OnError,
            onComplete: () =>
            {
                subscriber.OnNext(noMatchResult);
                subscriber.OnCompleted();
            });

        return src.Subscribe(innerSubscriber);
    }
}
