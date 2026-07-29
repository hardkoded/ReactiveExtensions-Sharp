namespace ReactiveExtensionsSharp.Operators;

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
            // Built directly and registered as a child of `subscriber` *before* subscribing (see
            // OperatorHelper.SubscribeChild's doc comment for why) so a downstream disposal cascades up and
            // stops a fully-synchronous source mid-loop, instead of only once the whole synchronous call stack
            // unwinds. Unlike SubscribeChild's target operators, this inner subscriber is only live for the
            // "before the first error" phase — once the source errors, it's Remove()'d, and the replacement
            // observable is subscribed with `subscriber` itself (not a fresh wrapper), so no further Add/Remove
            // bookkeeping is needed: the replacement is naturally coupled to the same downstream subscriber.
            Subscriber<T> sourceSubscriber = null!;
            sourceSubscriber = Subscriber.Create<T>(
                onNext: subscriber.OnNext,
                onError: error =>
                {
                    subscriber.Remove(sourceSubscriber);

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

                    replacement.Subscribe(subscriber);
                },
                onComplete: () =>
                {
                    subscriber.Remove(sourceSubscriber);
                    subscriber.OnCompleted();
                });

            subscriber.Add(sourceSubscriber);
            src.Subscribe(sourceSubscriber);

            return null;
        });
}
