namespace ReactiveExtensionsSharp.Operators;

/// <summary>Implements the <c>TakeWhile</c> operator. Mirrors rxjs's <c>takeWhile</c>.</summary>
public static class TakeWhileOperator
{
    /// <summary>
    /// Emits values from <paramref name="source"/> for as long as <paramref name="predicate"/> returns
    /// <see langword="true"/>, then completes as soon as it returns <see langword="false"/> without waiting
    /// for <paramref name="source"/> itself to complete.
    /// </summary>
    /// <remarks>Equivalent to the indexed overload with the index ignored. If <paramref name="predicate"/> throws, the exception is forwarded via <c>OnError</c>.</remarks>
    /// <typeparam name="T">The element type of the source observable.</typeparam>
    /// <param name="source">The source observable to take values from.</param>
    /// <param name="predicate">A function that tests each value.</param>
    /// <param name="inclusive">If <see langword="true"/>, the value that made <paramref name="predicate"/> return <see langword="false"/> is emitted before completing.</param>
    /// <returns>An observable of the values from <paramref name="source"/> up to (and, if <paramref name="inclusive"/>, including) the first one that fails <paramref name="predicate"/>.</returns>
    public static Observable<T> TakeWhile<T>(this Observable<T> source, Func<T, bool> predicate, bool inclusive = false)
        => source.TakeWhile((value, _) => predicate(value), inclusive);

    /// <summary>
    /// Emits values from <paramref name="source"/> for as long as <paramref name="predicate"/>, called with the
    /// value and its zero-based emission index, returns <see langword="true"/>, then completes as soon as it
    /// returns <see langword="false"/> without waiting for <paramref name="source"/> itself to complete.
    /// </summary>
    /// <remarks>If <paramref name="predicate"/> throws, the exception is forwarded via <c>OnError</c>.</remarks>
    /// <typeparam name="T">The element type of the source observable.</typeparam>
    /// <param name="source">The source observable to take values from.</param>
    /// <param name="predicate">A function that tests each value together with its index since subscription.</param>
    /// <param name="inclusive">If <see langword="true"/>, the value that made <paramref name="predicate"/> return <see langword="false"/> is emitted before completing.</param>
    /// <returns>An observable of the values from <paramref name="source"/> up to (and, if <paramref name="inclusive"/>, including) the first one that fails <paramref name="predicate"/>.</returns>
    public static Observable<T> TakeWhile<T>(this Observable<T> source, Func<T, int, bool> predicate, bool inclusive = false)
        => source.Operate<T, T>((src, subscriber) =>
        {
            var index = 0;

            // Built directly (rather than via the Subscribe(onNext:...) convenience overload) so this closure
            // holds a reference to the inner subscriber it can dispose immediately, even from a nested/
            // synchronous callback -- see Take.cs for the full explanation of why this matters for a
            // hand-rolled, self-checking synchronous source.
            Subscriber<T> innerSubscriber = null!;
            innerSubscriber = Subscriber.Create<T>(
                onNext: value =>
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

                    if (result || inclusive)
                    {
                        subscriber.OnNext(value);
                    }

                    if (!result)
                    {
                        subscriber.OnCompleted();
                        innerSubscriber.Dispose();
                    }
                },
                onError: subscriber.OnError,
                onComplete: subscriber.OnCompleted);

            return src.Subscribe(innerSubscriber);
        });
}
