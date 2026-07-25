namespace RxSharp.Operators;

/// <summary>Extension methods implementing the <c>take</c> operator.</summary>
public static class TakeOperator
{
    /// <summary>
    /// Emits only the first <paramref name="count"/> values from <paramref name="source"/>, then completes
    /// and unsubscribes from <paramref name="source"/> — regardless of whether <paramref name="source"/>
    /// itself ever completes. If <paramref name="source"/> emits fewer than <paramref name="count"/> values,
    /// all of them are emitted and the output completes whenever <paramref name="source"/> does.
    /// </summary>
    /// <typeparam name="T">The type of values emitted by <paramref name="source"/>.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="count">The maximum number of values to emit.</param>
    /// <returns>
    /// An observable that emits at most <paramref name="count"/> values from <paramref name="source"/>.
    /// If <paramref name="count"/> is zero or less, an empty, already-completed observable is returned and
    /// <paramref name="source"/> is never subscribed to.
    /// </returns>
    public static Observable<T> Take<T>(this Observable<T> source, int count)
    {
        if (count <= 0)
        {
            return Observable.Empty<T>();
        }

        return source.Operate<T, T>((src, subscriber) =>
        {
            var seen = 0;
            var sourceSubscription = new SingleAssignmentDisposable();
            sourceSubscription.Disposable = src.Subscribe(
                onNext: value =>
                {
                    if (++seen <= count)
                    {
                        subscriber.OnNext(value);
                        if (seen >= count)
                        {
                            subscriber.OnCompleted();
                            sourceSubscription.Dispose();
                        }
                    }
                },
                onError: subscriber.OnError,
                onComplete: subscriber.OnCompleted);

            return sourceSubscription;
        });
    }
}
