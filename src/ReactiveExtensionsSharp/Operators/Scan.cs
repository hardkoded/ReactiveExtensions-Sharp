namespace ReactiveExtensionsSharp.Operators;

/// <summary>Extension methods implementing the <c>scan</c> operator.</summary>
/// <remarks>
/// Unlike <see cref="MergeScanOperator"/>, <c>scan</c>'s accumulator returns a plain value synchronously
/// instead of an inner observable, so every accumulation happens inline with no concurrency to manage.
/// </remarks>
public static class ScanOperator
{
    /// <summary>
    /// Applies <paramref name="accumulator"/> to <paramref name="seed"/> and each value emitted by
    /// <paramref name="source"/>, along with its zero-based index, emitting the updated accumulator value after
    /// every source emission.
    /// </summary>
    /// <remarks>
    /// If <paramref name="accumulator"/> throws, the exception is forwarded to the subscriber via <c>OnError</c>
    /// instead of propagating synchronously.
    /// </remarks>
    /// <typeparam name="TSource">The type of values emitted by <paramref name="source"/>.</typeparam>
    /// <typeparam name="TAcc">The type of the accumulated state and of the output values.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="accumulator">
    /// A function called with the current accumulated state, the source value, and its index (starting at 0);
    /// returns the new accumulated state.
    /// </param>
    /// <param name="seed">The initial accumulated state.</param>
    /// <returns>An observable of the running accumulator value after each source emission.</returns>
    public static Observable<TAcc> Scan<TSource, TAcc>(this Observable<TSource> source, Func<TAcc, TSource, int, TAcc> accumulator, TAcc seed)
        => source.Operate<TSource, TAcc>((src, subscriber) =>
        {
            var acc = seed;
            var index = 0;

            return src.SubscribeChild(
                subscriber,
                onNext: value =>
                {
                    TAcc result;
                    try
                    {
                        result = accumulator(acc, value, index++);
                    }
                    catch (Exception ex)
                    {
                        subscriber.OnError(ex);
                        return;
                    }

                    acc = result;
                    subscriber.OnNext(acc);
                },
                onError: subscriber.OnError,
                onComplete: subscriber.OnCompleted);
        });

    /// <summary>
    /// Applies <paramref name="accumulator"/> to <paramref name="seed"/> and each value emitted by
    /// <paramref name="source"/>, emitting the updated accumulator value after every source emission.
    /// </summary>
    /// <remarks>Equivalent to the indexed overload with the index ignored. See its remarks for full behavior.</remarks>
    /// <typeparam name="TSource">The type of values emitted by <paramref name="source"/>.</typeparam>
    /// <typeparam name="TAcc">The type of the accumulated state and of the output values.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="accumulator">A function called with the current accumulated state and the source value; returns the new accumulated state.</param>
    /// <param name="seed">The initial accumulated state.</param>
    /// <returns>An observable of the running accumulator value after each source emission.</returns>
    public static Observable<TAcc> Scan<TSource, TAcc>(this Observable<TSource> source, Func<TAcc, TSource, TAcc> accumulator, TAcc seed)
        => source.Scan((acc, value, _) => accumulator(acc, value), seed);

    /// <summary>
    /// Applies <paramref name="accumulator"/> to each value emitted by <paramref name="source"/>, along with
    /// its zero-based index, emitting the updated accumulator value after every source emission after the
    /// first. There is no seed: the first value emitted by <paramref name="source"/> becomes the initial
    /// accumulated state and is emitted as-is, without ever being passed to <paramref name="accumulator"/>.
    /// </summary>
    /// <remarks>
    /// Because the first value is used directly as the seed, <paramref name="accumulator"/> is first called
    /// (with index <c>1</c>) on the second source value, not the first. If <paramref name="accumulator"/>
    /// throws, the exception is forwarded to the subscriber via <c>OnError</c> instead of propagating
    /// synchronously. If <paramref name="source"/> completes without ever emitting, the output simply
    /// completes without ever emitting either (there being no first value to seed with).
    /// </remarks>
    /// <typeparam name="T">The type of values emitted by <paramref name="source"/> and of the output.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="accumulator">
    /// A function called with the current accumulated state, the source value, and its index (starting at 1,
    /// since index 0 is consumed as the seed); returns the new accumulated state.
    /// </param>
    /// <returns>An observable of the running accumulator value after each source emission but the first.</returns>
    public static Observable<T> Scan<T>(this Observable<T> source, Func<T, T, int, T> accumulator)
        => source.Operate<T, T>((src, subscriber) =>
        {
            var hasSeed = false;
            T acc = default!;
            var index = 0;

            return src.SubscribeChild(
                subscriber,
                onNext: value =>
                {
                    if (!hasSeed)
                    {
                        hasSeed = true;
                        acc = value;
                        index++;
                        subscriber.OnNext(acc);
                        return;
                    }

                    T result;
                    try
                    {
                        result = accumulator(acc, value, index++);
                    }
                    catch (Exception ex)
                    {
                        subscriber.OnError(ex);
                        return;
                    }

                    acc = result;
                    subscriber.OnNext(acc);
                },
                onError: subscriber.OnError,
                onComplete: subscriber.OnCompleted);
        });

    /// <summary>
    /// Applies <paramref name="accumulator"/> to each value emitted by <paramref name="source"/>, emitting the
    /// updated accumulator value after every source emission after the first. There is no seed: the first value
    /// emitted by <paramref name="source"/> becomes the initial accumulated state and is emitted as-is, without
    /// ever being passed to <paramref name="accumulator"/>.
    /// </summary>
    /// <remarks>Equivalent to the indexed overload with the index ignored. See its remarks for full behavior.</remarks>
    /// <typeparam name="T">The type of values emitted by <paramref name="source"/> and of the output.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="accumulator">A function called with the current accumulated state and the source value; returns the new accumulated state.</param>
    /// <returns>An observable of the running accumulator value after each source emission but the first.</returns>
    public static Observable<T> Scan<T>(this Observable<T> source, Func<T, T, T> accumulator)
        => source.Scan((acc, value, _) => accumulator(acc, value));
}
