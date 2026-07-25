namespace RxSharp.Operators;

/// <summary>Implements the <c>DelayWhen</c> operator. Mirrors rxjs's <c>delayWhen</c>.</summary>
public static class DelayWhenOperator
{
    /// <summary>
    /// Delays each value from <paramref name="source"/> until the "duration" observable produced for it by
    /// <paramref name="delayDurationSelector"/> emits its first value.
    /// </summary>
    /// <remarks>
    /// <paramref name="delayDurationSelector"/> is called with each source value and its zero-based index; the
    /// duration observable it returns is subscribed to immediately, but only its first emission (via an internal
    /// <c>Take(1)</c>) actually releases the corresponding source value downstream. If a duration observable
    /// completes without ever emitting, the source value it was paired with is silently dropped — it never
    /// reaches the subscriber. Because durations are handled with unbounded concurrency (via <c>MergeMap</c>),
    /// values can be released out of the order their durations were scheduled if a later duration fires before
    /// an earlier one — this operator does not preserve source order the way <c>Delay</c> does. If
    /// <paramref name="delayDurationSelector"/> throws, or a duration observable errors, the error is forwarded
    /// via <c>OnError</c>.
    /// </remarks>
    /// <typeparam name="T">The element type of the source observable.</typeparam>
    /// <typeparam name="TDuration">
    /// The element type of the per-value "duration" observable; only used to detect its first emission; the
    /// actual value carried is never inspected.
    /// </typeparam>
    /// <param name="source">The source observable whose values are delayed.</param>
    /// <param name="delayDurationSelector">
    /// A function that, given a source value and its index, returns an observable whose first emission signals
    /// that the value may now be forwarded.
    /// </param>
    /// <returns>An observable of the source values, each released once its corresponding duration observable emits.</returns>
    public static Observable<T> DelayWhen<T, TDuration>(this Observable<T> source, Func<T, int, Observable<TDuration>> delayDurationSelector)
        => source.MergeMap((value, index) => delayDurationSelector(value, index).Take(1).Map(_ => value));

    /// <summary>
    /// Delays each value from <paramref name="source"/> until the "duration" observable produced for it by
    /// <paramref name="delayDurationSelector"/> emits its first value.
    /// </summary>
    /// <remarks>Equivalent to the indexed overload with the index ignored. See its remarks for full behavior.</remarks>
    /// <typeparam name="T">The element type of the source observable.</typeparam>
    /// <typeparam name="TDuration">
    /// The element type of the per-value "duration" observable; only used to detect its first emission; the
    /// actual value carried is never inspected.
    /// </typeparam>
    /// <param name="source">The source observable whose values are delayed.</param>
    /// <param name="delayDurationSelector">
    /// A function that, given a source value, returns an observable whose first emission signals that the value
    /// may now be forwarded.
    /// </param>
    /// <returns>An observable of the source values, each released once its corresponding duration observable emits.</returns>
    public static Observable<T> DelayWhen<T, TDuration>(this Observable<T> source, Func<T, Observable<TDuration>> delayDurationSelector)
        => source.DelayWhen((value, _) => delayDurationSelector(value));
}
