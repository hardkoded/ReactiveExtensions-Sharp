namespace ReactiveExtensionsSharp.Operators;

/// <summary>Extension methods implementing the <c>tap</c> operator.</summary>
public static class TapOperator
{
    /// <summary>
    /// Mirrors <paramref name="source"/> exactly, invoking the supplied callbacks as side effects for each
    /// notification before forwarding that same notification downstream. Any callback left <see langword="null"/>
    /// is simply skipped for that kind of notification.
    /// </summary>
    /// <remarks>
    /// If a callback throws, the output errors with that exception via <c>OnError</c> instead of forwarding the
    /// original notification — for example, if <paramref name="onNext"/> throws, the source value is not
    /// forwarded and the thrown exception is emitted as an error instead; if <paramref name="onError"/> throws
    /// while handling the source's original error, the new exception is forwarded and the original error is
    /// dropped. Unlike rxjs's <c>tap</c>, there are no separate hooks for subscribe/unsubscribe/finalize —
    /// only <paramref name="onNext"/>, <paramref name="onError"/>, and <paramref name="onComplete"/> are supported.
    /// </remarks>
    /// <typeparam name="T">The type of values emitted by <paramref name="source"/>.</typeparam>
    /// <param name="source">The source sequence to mirror.</param>
    /// <param name="onNext">An optional side effect invoked with each value before it is forwarded downstream.</param>
    /// <param name="onError">An optional side effect invoked with the error before it is forwarded downstream.</param>
    /// <param name="onComplete">An optional side effect invoked before completion is forwarded downstream.</param>
    /// <returns>An observable that mirrors <paramref name="source"/> while running the given side effects.</returns>
    public static Observable<T> Tap<T>(this Observable<T> source, Action<T>? onNext = null, Action<Exception>? onError = null, Action? onComplete = null)
        => source.Operate<T, T>((src, subscriber) => src.SubscribeChild(
            subscriber,
            onNext: value =>
            {
                if (onNext is not null)
                {
                    try
                    {
                        onNext(value);
                    }
                    catch (Exception ex)
                    {
                        subscriber.OnError(ex);
                        return;
                    }
                }

                subscriber.OnNext(value);
            },
            onError: error =>
            {
                if (onError is not null)
                {
                    try
                    {
                        onError(error);
                    }
                    catch (Exception ex)
                    {
                        subscriber.OnError(ex);
                        return;
                    }
                }

                subscriber.OnError(error);
            },
            onComplete: () =>
            {
                if (onComplete is not null)
                {
                    try
                    {
                        onComplete();
                    }
                    catch (Exception ex)
                    {
                        subscriber.OnError(ex);
                        return;
                    }
                }

                subscriber.OnCompleted();
            }));
}
