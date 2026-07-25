namespace RxSharp.Operators;

/// <summary>Extension methods implementing the <c>throwIfEmpty</c> operator.</summary>
public static class ThrowIfEmptyOperator
{
    /// <summary>
    /// Mirrors <paramref name="source"/>, but if it completes without ever emitting a value, the output errors
    /// instead of completing. If at least one value was emitted, the output completes normally.
    /// </summary>
    /// <typeparam name="T">The type of values emitted by <paramref name="source"/>.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="errorFactory">
    /// An optional factory invoked to create the exception to emit if <paramref name="source"/> completes
    /// without emitting a value. Defaults to a new <see cref="EmptyErrorException"/>.
    /// </param>
    /// <returns>
    /// An observable that mirrors <paramref name="source"/>, except that an empty completion is replaced with
    /// an error produced by <paramref name="errorFactory"/>.
    /// </returns>
    public static Observable<T> ThrowIfEmpty<T>(this Observable<T> source, Func<Exception>? errorFactory = null)
    {
        var makeError = errorFactory ?? (() => new EmptyErrorException());
        return source.Operate<T, T>((src, subscriber) =>
        {
            var hasValue = false;
            return src.SubscribeChild(
                subscriber,
                onNext: value =>
                {
                    hasValue = true;
                    subscriber.OnNext(value);
                },
                onError: subscriber.OnError,
                onComplete: () =>
                {
                    if (hasValue)
                    {
                        subscriber.OnCompleted();
                    }
                    else
                    {
                        subscriber.OnError(makeError());
                    }
                });
        });
    }
}
