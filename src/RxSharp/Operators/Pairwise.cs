namespace RxSharp.Operators;

/// <summary>Extension methods implementing the <c>pairwise</c> operator.</summary>
public static class PairwiseOperator
{
    /// <summary>
    /// Emits a tuple of each value emitted by <paramref name="source"/> together with the value that preceded
    /// it. The very first value has no predecessor, so it is buffered but not emitted on its own; the first
    /// emitted tuple pairs the first and second source values.
    /// </summary>
    /// <typeparam name="T">The type of values emitted by <paramref name="source"/>.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <returns>
    /// An observable of <c>(Previous, Current)</c> tuples, one per source emission after the first. If
    /// <paramref name="source"/> emits at most one value before completing, the output completes without ever
    /// emitting.
    /// </returns>
    public static Observable<(T Previous, T Current)> Pairwise<T>(this Observable<T> source)
        => source.Operate<T, (T Previous, T Current)>((src, subscriber) =>
        {
            var hasPrevious = false;
            T previous = default!;

            return src.SubscribeChild(
                subscriber,
                onNext: value =>
                {
                    // The previous value is captured and the state updated to the new value *before* emitting
                    // — not after — so a reentrant call triggered synchronously by the OnNext below (e.g. a
                    // subscriber that immediately pushes another value back into a Subject source) observes the
                    // correct, already-updated previous value rather than a stale one.
                    var wasPresent = hasPrevious;
                    var priorValue = previous;
                    hasPrevious = true;
                    previous = value;

                    if (wasPresent)
                    {
                        subscriber.OnNext((priorValue, value));
                    }
                },
                onError: subscriber.OnError,
                onComplete: subscriber.OnCompleted);
        });
}
