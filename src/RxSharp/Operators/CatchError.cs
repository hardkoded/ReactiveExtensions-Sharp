namespace RxSharp.Operators;

/// <summary>Implements the <c>CatchError</c> operator. Mirrors rxjs's <c>catchError</c>.</summary>
public static class CatchErrorOperator
{
    /// <summary>
    /// Recovers from an error on <paramref name="source"/> by subscribing to a replacement observable produced by
    /// <paramref name="selector"/>, instead of propagating the error to the subscriber.
    /// </summary>
    /// <remarks>
    /// Values and completion from <paramref name="source"/> pass through unchanged; only an error notification is
    /// intercepted. If <paramref name="selector"/> itself throws, that exception is forwarded via <c>OnError</c>
    /// instead of the original one.
    /// </remarks>
    /// <typeparam name="T">The element type of both the source and the replacement observable.</typeparam>
    /// <param name="source">The source observable to catch errors from.</param>
    /// <param name="selector">A function that maps the error to a replacement observable to continue with.</param>
    /// <returns>
    /// An observable that mirrors <paramref name="source"/> until it errors, then mirrors the replacement
    /// observable produced by <paramref name="selector"/>.
    /// </returns>
    public static Observable<T> CatchError<T>(this Observable<T> source, Func<Exception, Observable<T>> selector)
        => source.Operate<T, T>((src, subscriber) =>
        {
            var subscription = new SingleAssignmentDisposable();
            subscription.Disposable = src.Subscribe(
                onNext: subscriber.OnNext,
                onError: error =>
                {
                    Observable<T> replacement;
                    try
                    {
                        replacement = selector(error);
                    }
                    catch (Exception ex)
                    {
                        subscriber.OnError(ex);
                        return;
                    }

                    subscription.Disposable = replacement.Subscribe(subscriber);
                },
                onComplete: subscriber.OnCompleted);

            return subscription;
        });
}
