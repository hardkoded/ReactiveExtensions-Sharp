using RxSharp.Operators;

namespace RxSharp.Extras;

/// <summary>Extension methods racing a single subscription against cancellation and a timeout.</summary>
public static partial class Extensions
{
    /// <summary>
    /// Races <paramref name="source"/> against cancellation and a timeout, whichever fires first. The
    /// non-retrying half of <see cref="Extensions.RetryAndRaceWithSignalAndTimer{T}(Observable{T}, TimeSpan, Func{Exception}, TimeSpan?, CancellationToken)"/>
    /// - use this directly for a single wait (e.g. "wait for the next matching event") that doesn't need
    /// retrying, and reach for the retrying combinator when it does.
    /// </summary>
    /// <typeparam name="T">The type of values produced by <paramref name="source"/>.</typeparam>
    /// <param name="source">The source sequence to race.</param>
    /// <param name="timeout">The overall duration before giving up with a timeout error. A zero or negative value disables the timeout.</param>
    /// <param name="causeFactory">
    /// Produces the exception used for both the cancellation and timeout branches. Defaults to
    /// <see cref="OperationCanceledException"/> for cancellation and <see cref="TimeoutException"/> for the timeout,
    /// via the defaults of <see cref="Extensions.FromCancellationToken"/> and <see cref="Extensions.Timeout"/> respectively.
    /// Since one factory covers both branches, a caller needing to tell the two apart by exception type should
    /// pass <see langword="null"/> here (so each branch keeps its own distinct default type) and catch/rethrow
    /// as needed at the call site.
    /// </param>
    /// <param name="cancellationToken">A token that, when cancelled, aborts the wait immediately.</param>
    /// <returns>An observable that mirrors <paramref name="source"/> unless the timeout or cancellation fires first.</returns>
    public static Observable<T> RaceWithSignalAndTimer<T>(
        this Observable<T> source,
        TimeSpan timeout,
        Func<Exception>? causeFactory,
        CancellationToken cancellationToken)
        => source.RaceWith(
            Extensions.FromCancellationToken(cancellationToken, causeFactory).AssumeNeverEmits<T>(),
            Extensions.Timeout(timeout, causeFactory).AssumeNeverEmits<T>());

    /// <summary>
    /// Overload of <see cref="RaceWithSignalAndTimer{T}(Observable{T}, TimeSpan, Func{Exception}, CancellationToken)"/>
    /// using the default cause factory (<see cref="OperationCanceledException"/>/<see cref="TimeoutException"/>).
    /// </summary>
    /// <typeparam name="T">The type of values produced by <paramref name="source"/>.</typeparam>
    /// <param name="source">The source sequence to race.</param>
    /// <param name="timeout">The overall duration before giving up with a timeout error.</param>
    /// <param name="cancellationToken">A token that, when cancelled, aborts the wait immediately.</param>
    /// <returns>An observable that mirrors <paramref name="source"/> unless the timeout or cancellation fires first.</returns>
    public static Observable<T> RaceWithSignalAndTimer<T>(this Observable<T> source, TimeSpan timeout, CancellationToken cancellationToken)
        => source.RaceWithSignalAndTimer(timeout, causeFactory: null, cancellationToken);
}
