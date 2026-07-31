namespace ReactiveExtensionsSharp.Extras;

/// <summary>Extension methods racing a single subscription against cancellation/an external signal and a timeout.</summary>
public static partial class RxExtensions
{
    /// <summary>
    /// Races <paramref name="source"/>'s first value against cancellation and a timeout, whichever fires first.
    /// The non-retrying half of <see cref="RetryAndRaceWithSignalAndTimer{T}(Observable{T}, TimeSpan, Func{Exception}, TimeSpan?, CancellationToken)"/>
    /// - use this directly for a single wait (e.g. "wait for the next matching event") that doesn't need
    /// retrying, and reach for the retrying combinator when it does. Races at the <see cref="Task"/> level
    /// rather than the <see cref="Observable{T}"/> level: cancellation and the timeout never produce a value
    /// of type <typeparamref name="T"/>, only ever fault or never complete, and C# has no bottom type to make
    /// an error-only <c>Observable&lt;Unit&gt;</c> type-check as <c>Observable&lt;T&gt;</c> the way TypeScript's
    /// <c>never</c> lets rxjs do it - racing plain <see cref="Task"/>s sidesteps that entirely.
    /// </summary>
    /// <typeparam name="T">The type of values produced by <paramref name="source"/>.</typeparam>
    /// <param name="source">The source sequence to race.</param>
    /// <param name="timeout">The overall duration before giving up with a timeout error. A zero or negative value disables the timeout.</param>
    /// <param name="causeFactory">
    /// Produces the exception used for both the cancellation and timeout branches. Defaults to
    /// <see cref="OperationCanceledException"/> for cancellation and <see cref="TimeoutException"/> for the timeout.
    /// Since one factory covers both branches, a caller needing to tell the two apart by exception type should
    /// pass <see langword="null"/> here (so each branch keeps its own distinct default type) and catch/rethrow
    /// as needed at the call site.
    /// </param>
    /// <param name="cancellationToken">A token that, when cancelled, aborts the wait immediately.</param>
    /// <returns>A task that resolves to <paramref name="source"/>'s first value unless the timeout or cancellation fires first.</returns>
    public static async Task<T> RaceWithSignalAndTimer<T>(
        this Observable<T> source,
        TimeSpan timeout,
        Func<Exception>? causeFactory,
        CancellationToken cancellationToken)
    {
        var makeCancelCause = causeFactory ?? DefaultCancellationCause;
        var cancelTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var registration = cancellationToken.Register(() => cancelTcs.TrySetException(makeCancelCause()));

        return await source.RaceWithSignalAndTimer(timeout, causeFactory, cancelTcs.Task).ConfigureAwait(false);
    }

    /// <summary>
    /// Overload of <see cref="RaceWithSignalAndTimer{T}(Observable{T}, TimeSpan, Func{Exception}, CancellationToken)"/>
    /// using the default cause factory (<see cref="OperationCanceledException"/>/<see cref="TimeoutException"/>).
    /// </summary>
    /// <typeparam name="T">The type of values produced by <paramref name="source"/>.</typeparam>
    /// <param name="source">The source sequence to race.</param>
    /// <param name="timeout">The overall duration before giving up with a timeout error.</param>
    /// <param name="cancellationToken">A token that, when cancelled, aborts the wait immediately.</param>
    /// <returns>A task that resolves to <paramref name="source"/>'s first value unless the timeout or cancellation fires first.</returns>
    public static Task<T> RaceWithSignalAndTimer<T>(this Observable<T> source, TimeSpan timeout, CancellationToken cancellationToken)
        => source.RaceWithSignalAndTimer(timeout, causeFactory: null, cancellationToken);

    /// <summary>
    /// Races <paramref name="source"/>'s first value against an already-existing <paramref name="signal"/> task
    /// and a timeout, whichever fires first. Use this instead of the <see cref="CancellationToken"/> overload
    /// when the "give up" condition already exists as a task elsewhere (e.g. a task that faults when a session
    /// closes), rather than one this combinator needs to build.
    /// </summary>
    /// <remarks>
    /// <paramref name="signal"/> is expected, by contract, to only ever fault or never complete. If it completes
    /// without faulting, that is a contract violation in the caller and surfaces as an
    /// <see cref="InvalidOperationException"/> rather than silently returning a value.
    /// </remarks>
    /// <typeparam name="T">The type of values produced by <paramref name="source"/>.</typeparam>
    /// <param name="source">The source sequence to race.</param>
    /// <param name="timeout">The overall duration before giving up with a timeout error. A zero or negative value disables the timeout.</param>
    /// <param name="causeFactory">Produces the exception thrown once <paramref name="timeout"/> elapses. Defaults to a new <see cref="TimeoutException"/>.</param>
    /// <param name="signal">An already-existing task that, by contract, only ever faults or never completes.</param>
    /// <returns>A task that resolves to <paramref name="source"/>'s first value unless the timeout or <paramref name="signal"/> fires first.</returns>
    public static async Task<T> RaceWithSignalAndTimer<T>(
        this Observable<T> source,
        TimeSpan timeout,
        Func<Exception>? causeFactory,
        Task signal)
    {
        var makeTimeoutCause = causeFactory ?? DefaultTimeoutCause;

        var sourceTcs = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var subscription = source.Subscribe(
            onNext: value => sourceTcs.TrySetResult(value),
            onError: err => sourceTcs.TrySetException(err));

        using var timeoutCts = new CancellationTokenSource();
        var timeoutTask = Task.Delay(
            timeout > TimeSpan.Zero ? timeout : System.Threading.Timeout.InfiniteTimeSpan,
            timeoutCts.Token);

        try
        {
            var winner = await Task.WhenAny(sourceTcs.Task, signal, timeoutTask).ConfigureAwait(false);
            if (winner == sourceTcs.Task)
            {
                return await sourceTcs.Task.ConfigureAwait(false);
            }

            if (winner == signal)
            {
                if (signal.IsFaulted)
                {
                    await signal.ConfigureAwait(false);
                }

                throw new InvalidOperationException(
                    "RaceWithSignalAndTimer: the signal task completed without faulting, but was assumed to only ever fault or never complete.");
            }

            throw makeTimeoutCause();
        }
        finally
        {
            // Stops a losing timer/delay promptly instead of leaving it scheduled until it would have
            // naturally elapsed; nothing observes timeoutTask after this, so its resulting cancellation
            // (or, if it already won, its own exception) is discarded safely.
            timeoutCts.Cancel();
        }
    }
}
