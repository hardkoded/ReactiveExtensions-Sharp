namespace RxSharp.Operators;

/// <summary>Implements the <c>Every</c> operator. Mirrors rxjs's <c>every</c>.</summary>
public static class EveryOperator
{
    /// <summary>
    /// Emits a single boolean: <see langword="true"/> if every value from <paramref name="source"/> satisfies
    /// <paramref name="predicate"/> (checked once <paramref name="source"/> completes), or <see langword="false"/>
    /// as soon as one value fails it &#8212; completing immediately at that point without waiting for
    /// <paramref name="source"/> itself to complete.
    /// </summary>
    /// <remarks>If <paramref name="predicate"/> throws, the exception is forwarded via <c>OnError</c>.</remarks>
    /// <typeparam name="T">The element type of the source observable.</typeparam>
    /// <param name="source">The source observable to test.</param>
    /// <param name="predicate">A function that tests each value.</param>
    /// <returns>An observable that emits a single boolean, then completes.</returns>
    public static Observable<bool> Every<T>(this Observable<T> source, Func<T, bool> predicate)
        => source.Every((value, _) => predicate(value));

    /// <summary>
    /// Emits a single boolean: <see langword="true"/> if every value from <paramref name="source"/> satisfies
    /// <paramref name="predicate"/> (called with the value and its zero-based emission index, checked once
    /// <paramref name="source"/> completes), or <see langword="false"/> as soon as one value fails it &#8212;
    /// completing immediately at that point without waiting for <paramref name="source"/> itself to complete.
    /// </summary>
    /// <remarks>If <paramref name="predicate"/> throws, the exception is forwarded via <c>OnError</c>.</remarks>
    /// <typeparam name="T">The element type of the source observable.</typeparam>
    /// <param name="source">The source observable to test.</param>
    /// <param name="predicate">A function that tests each value together with its index since subscription.</param>
    /// <returns>An observable that emits a single boolean, then completes.</returns>
    public static Observable<bool> Every<T>(this Observable<T> source, Func<T, int, bool> predicate)
        => source.Operate<T, bool>((src, subscriber) =>
        {
            var index = 0;

            // Built directly (see Take.cs for the full explanation) so this closure can dispose the inner
            // subscriber immediately, even from within a nested/synchronous source callback.
            Subscriber<T> innerSubscriber = null!;
            innerSubscriber = Subscriber.Create<T>(
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

                    if (!matches)
                    {
                        subscriber.OnNext(false);
                        subscriber.OnCompleted();
                        innerSubscriber.Dispose();
                    }
                },
                onError: subscriber.OnError,
                onComplete: () =>
                {
                    subscriber.OnNext(true);
                    subscriber.OnCompleted();
                });

            return src.Subscribe(innerSubscriber);
        });
}
