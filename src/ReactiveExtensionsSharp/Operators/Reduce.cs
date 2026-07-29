namespace ReactiveExtensionsSharp.Operators;

/// <summary>Extension methods implementing the <c>reduce</c> operator.</summary>
/// <remarks>
/// Behaves like <see cref="ScanOperator"/>, except only the final accumulated value is emitted, once
/// <c>source</c> completes, rather than the running value after every emission. Implemented standalone
/// (not as <c>Scan(...).Last()</c>) since this port has no <c>Last</c> operator yet.
/// </remarks>
public static class ReduceOperator
{
    /// <summary>
    /// Applies <paramref name="accumulator"/> to <paramref name="seed"/> and each value emitted by
    /// <paramref name="source"/>, along with its zero-based index, emitting only the final accumulated value
    /// once <paramref name="source"/> completes.
    /// </summary>
    /// <remarks>
    /// If <paramref name="source"/> completes without ever emitting, the final value is <paramref name="seed"/>
    /// itself. If <paramref name="accumulator"/> throws, the exception is forwarded to the subscriber via
    /// <c>OnError</c> instead of propagating synchronously, and no value is ever emitted.
    /// </remarks>
    /// <typeparam name="TSource">The type of values emitted by <paramref name="source"/>.</typeparam>
    /// <typeparam name="TAcc">The type of the accumulated state and of the output value.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="accumulator">
    /// A function called with the current accumulated state, the source value, and its index (starting at 0);
    /// returns the new accumulated state.
    /// </param>
    /// <param name="seed">The initial accumulated state.</param>
    /// <returns>An observable that emits the final accumulated value once <paramref name="source"/> completes.</returns>
    public static Observable<TAcc> Reduce<TSource, TAcc>(this Observable<TSource> source, Func<TAcc, TSource, int, TAcc> accumulator, TAcc seed)
        => source.Operate<TSource, TAcc>((src, subscriber) =>
        {
            var acc = seed;
            var index = 0;

            return src.SubscribeChild(
                subscriber,
                onNext: value =>
                {
                    try
                    {
                        acc = accumulator(acc, value, index++);
                    }
                    catch (Exception ex)
                    {
                        subscriber.OnError(ex);
                    }
                },
                onError: subscriber.OnError,
                onComplete: () =>
                {
                    subscriber.OnNext(acc);
                    subscriber.OnCompleted();
                });
        });

    /// <summary>
    /// Applies <paramref name="accumulator"/> to <paramref name="seed"/> and each value emitted by
    /// <paramref name="source"/>, emitting only the final accumulated value once <paramref name="source"/>
    /// completes.
    /// </summary>
    /// <remarks>Equivalent to the indexed overload with the index ignored. See its remarks for full behavior.</remarks>
    /// <typeparam name="TSource">The type of values emitted by <paramref name="source"/>.</typeparam>
    /// <typeparam name="TAcc">The type of the accumulated state and of the output value.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="accumulator">A function called with the current accumulated state and the source value; returns the new accumulated state.</param>
    /// <param name="seed">The initial accumulated state.</param>
    /// <returns>An observable that emits the final accumulated value once <paramref name="source"/> completes.</returns>
    public static Observable<TAcc> Reduce<TSource, TAcc>(this Observable<TSource> source, Func<TAcc, TSource, TAcc> accumulator, TAcc seed)
        => source.Reduce((acc, value, _) => accumulator(acc, value), seed);

    /// <summary>
    /// Applies <paramref name="accumulator"/> to each value emitted by <paramref name="source"/>, along with its
    /// zero-based index, emitting only the final accumulated value once <paramref name="source"/> completes.
    /// There is no seed: the first value emitted by <paramref name="source"/> becomes the initial accumulated
    /// state, without ever being passed to <paramref name="accumulator"/>.
    /// </summary>
    /// <remarks>
    /// If <paramref name="source"/> completes without ever emitting, the output completes without emitting any
    /// value either (there being no first value to seed with). Because the first value is used directly as the
    /// seed, <paramref name="accumulator"/> is first called (with index <c>1</c>) on the second source value,
    /// not the first. If <paramref name="accumulator"/> throws, the exception is forwarded via <c>OnError</c>.
    /// </remarks>
    /// <typeparam name="T">The type of values emitted by <paramref name="source"/> and of the output.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="accumulator">
    /// A function called with the current accumulated state, the source value, and its index (starting at 1,
    /// since index 0 is consumed as the seed); returns the new accumulated state.
    /// </param>
    /// <returns>An observable that emits the final accumulated value once <paramref name="source"/> completes, if it ever emitted.</returns>
    public static Observable<T> Reduce<T>(this Observable<T> source, Func<T, T, int, T> accumulator)
        => source.Operate<T, T>((src, subscriber) =>
        {
            var hasValue = false;
            T acc = default!;
            var index = 0;

            return src.SubscribeChild(
                subscriber,
                onNext: value =>
                {
                    if (!hasValue)
                    {
                        hasValue = true;
                        acc = value;
                        index++;
                        return;
                    }

                    try
                    {
                        acc = accumulator(acc, value, index++);
                    }
                    catch (Exception ex)
                    {
                        subscriber.OnError(ex);
                    }
                },
                onError: subscriber.OnError,
                onComplete: () =>
                {
                    if (hasValue)
                    {
                        subscriber.OnNext(acc);
                    }

                    subscriber.OnCompleted();
                });
        });

    /// <summary>
    /// Applies <paramref name="accumulator"/> to each value emitted by <paramref name="source"/>, emitting only
    /// the final accumulated value once <paramref name="source"/> completes. There is no seed: the first value
    /// emitted by <paramref name="source"/> becomes the initial accumulated state, without ever being passed to
    /// <paramref name="accumulator"/>.
    /// </summary>
    /// <remarks>Equivalent to the indexed overload with the index ignored. See its remarks for full behavior.</remarks>
    /// <typeparam name="T">The type of values emitted by <paramref name="source"/> and of the output.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="accumulator">A function called with the current accumulated state and the source value; returns the new accumulated state.</param>
    /// <returns>An observable that emits the final accumulated value once <paramref name="source"/> completes, if it ever emitted.</returns>
    public static Observable<T> Reduce<T>(this Observable<T> source, Func<T, T, T> accumulator)
        => source.Reduce((acc, value, _) => accumulator(acc, value));
}
