namespace RxSharp.Operators;

/// <summary>Extension methods implementing the <c>withLatestFrom</c> operator.</summary>
public static class WithLatestFromOperator
{
    /// <summary>
    /// Combines each value emitted by <paramref name="source"/> with the latest value from <paramref name="other"/>
    /// via <paramref name="resultSelector"/>, only when <paramref name="source"/> itself emits — <paramref name="other"/>
    /// emitting on its own never produces an output value.
    /// </summary>
    /// <remarks>
    /// No output is produced until <paramref name="other"/> has emitted at least once. <paramref name="other"/>
    /// is subscribed to before <paramref name="source"/>, so a synchronous <paramref name="other"/> already has
    /// a latest value by the time <paramref name="source"/>'s first (possibly also synchronous) emission arrives.
    /// <paramref name="other"/> completing has no effect on the output — its last known value keeps being used.
    /// If <paramref name="resultSelector"/> throws, or either observable errors, the error is forwarded via
    /// <c>OnError</c>. The output completes when <paramref name="source"/> completes, regardless of <paramref name="other"/>.
    /// </remarks>
    /// <typeparam name="TSource">The type of values emitted by <paramref name="source"/>.</typeparam>
    /// <typeparam name="TOther">The type of values emitted by <paramref name="other"/>.</typeparam>
    /// <typeparam name="TResult">The type of values produced by <paramref name="resultSelector"/>.</typeparam>
    /// <param name="source">The primary source sequence, driving each output emission.</param>
    /// <param name="other">The observable whose latest value is combined with each <paramref name="source"/> emission.</param>
    /// <param name="resultSelector">A function combining a <paramref name="source"/> value with the latest <paramref name="other"/> value.</param>
    /// <returns>An observable of the combined values, one per <paramref name="source"/> emission once <paramref name="other"/> has emitted at least once.</returns>
    public static Observable<TResult> WithLatestFrom<TSource, TOther, TResult>(
        this Observable<TSource> source,
        Observable<TOther> other,
        Func<TSource, TOther, TResult> resultSelector)
        => source.Operate<TSource, TResult>((src, subscriber) =>
        {
            var hasLatest = false;
            TOther latest = default!;

            subscriber.Add(other.Subscribe(
                onNext: value =>
                {
                    latest = value;
                    hasLatest = true;
                },
                onError: subscriber.OnError));

            return src.SubscribeChild(
                subscriber,
                onNext: value =>
                {
                    if (!hasLatest)
                    {
                        return;
                    }

                    TResult result;
                    try
                    {
                        result = resultSelector(value, latest);
                    }
                    catch (Exception ex)
                    {
                        subscriber.OnError(ex);
                        return;
                    }

                    subscriber.OnNext(result);
                },
                onError: subscriber.OnError,
                onComplete: subscriber.OnCompleted);
        });

    /// <summary>
    /// Combines each value emitted by <paramref name="source"/> with the latest value from <paramref name="other"/>
    /// as a tuple, only when <paramref name="source"/> itself emits.
    /// </summary>
    /// <remarks>Equivalent to the overload taking a <c>resultSelector</c>, using tuple construction as the selector. See its remarks for full behavior.</remarks>
    /// <typeparam name="TSource">The type of values emitted by <paramref name="source"/>.</typeparam>
    /// <typeparam name="TOther">The type of values emitted by <paramref name="other"/>.</typeparam>
    /// <param name="source">The primary source sequence, driving each output emission.</param>
    /// <param name="other">The observable whose latest value is combined with each <paramref name="source"/> emission.</param>
    /// <returns>An observable of <c>(Source, Other)</c> tuples, one per <paramref name="source"/> emission once <paramref name="other"/> has emitted at least once.</returns>
    public static Observable<(TSource Source, TOther Other)> WithLatestFrom<TSource, TOther>(this Observable<TSource> source, Observable<TOther> other)
        => source.WithLatestFrom(other, (value, otherValue) => (value, otherValue));

    /// <summary>
    /// Combines each value emitted by <paramref name="source"/> with the latest value from every observable in
    /// <paramref name="others"/>, as a list (source value first, then each other's latest value in order), only
    /// when <paramref name="source"/> itself emits. Same-type-only for now (unlike rxjs's heterogeneously-typed
    /// tuple overloads), mirroring how <see cref="Observable.Zip{T}"/>/<see cref="Observable.CombineLatest{T}"/>
    /// handle multiple same-type sources.
    /// </summary>
    /// <remarks>
    /// No output is produced until every observable in <paramref name="others"/> has emitted at least once. Each
    /// is subscribed to before <paramref name="source"/>, so synchronous ones already have a latest value by the
    /// time <paramref name="source"/>'s first (possibly also synchronous) emission arrives. Any of them completing
    /// has no effect on the output — its last known value keeps being used. If any observable errors, the error is
    /// forwarded via <c>OnError</c>. The output completes when <paramref name="source"/> completes, regardless of
    /// <paramref name="others"/>.
    /// </remarks>
    /// <typeparam name="T">The element type shared by <paramref name="source"/> and every observable in <paramref name="others"/>.</typeparam>
    /// <param name="source">The primary source sequence, driving each output emission.</param>
    /// <param name="others">The observables whose latest values are combined with each <paramref name="source"/> emission, in order.</param>
    /// <returns>
    /// An observable of lists — <c>[sourceValue, others[0]'s latest, others[1]'s latest, ...]</c> — one per
    /// <paramref name="source"/> emission once every observable in <paramref name="others"/> has emitted at least once.
    /// </returns>
    public static Observable<IReadOnlyList<T>> WithLatestFrom<T>(this Observable<T> source, params Observable<T>[] others)
        => source.Operate<T, IReadOnlyList<T>>((src, subscriber) =>
        {
            var latest = new T[others.Length];
            var hasLatest = new bool[others.Length];

            for (var i = 0; i < others.Length; i++)
            {
                var index = i;
                subscriber.Add(others[index].Subscribe(
                    onNext: value =>
                    {
                        latest[index] = value;
                        hasLatest[index] = true;
                    },
                    onError: subscriber.OnError));
            }

            return src.SubscribeChild(
                subscriber,
                onNext: value =>
                {
                    if (!Array.TrueForAll(hasLatest, has => has))
                    {
                        return;
                    }

                    var combined = new T[others.Length + 1];
                    combined[0] = value;
                    Array.Copy(latest, 0, combined, 1, others.Length);
                    subscriber.OnNext(combined);
                },
                onError: subscriber.OnError,
                onComplete: subscriber.OnCompleted);
        });
}
