using RxSharp.Operators;

namespace RxSharp.Extras;

/// <summary>Extension methods composing retry, cancellation, and timeout into the single combinator Puppeteer's <c>Locator</c> actions rely on.</summary>
public static partial class PuppeteerExtras
{
    /// <summary>
    /// The combinator behind Puppeteer's <c>Locator</c> actions (click/fill/hover/wait): retry the source
    /// on error, racing the whole thing against cancellation and a timeout. Mirrors Puppeteer's
    /// <c>retryAndRaceWithSignalAndTimer</c>: <c>pipe(retry({delay}), raceWith(fromAbortSignal(...), timeout(...)))</c>.
    /// This is what lets a Locator action keep re-attempting a flaky operation (e.g. "find and click an element
    /// that may not have rendered yet") while still giving up promptly, either because the caller cancelled it
    /// or because it took longer than <paramref name="timeout"/> — whichever happens first. Since the cancellation
    /// and timeout branches are <see cref="Observable{T}"/> sources that only ever error (see
    /// <see cref="PuppeteerExtras.FromCancellationToken"/> and <see cref="PuppeteerExtras.Timeout"/>), the only way
    /// this combinator produces a value is if the retried <paramref name="source"/> itself emits one before either
    /// branch fires.
    /// </summary>
    /// <typeparam name="T">The type of values produced by <paramref name="source"/>.</typeparam>
    /// <param name="source">The source sequence to retry (e.g. a single Locator attempt that may throw).</param>
    /// <param name="timeout">The overall duration before the action is abandoned with a timeout error. A zero or negative value disables the timeout.</param>
    /// <param name="causeFactory">
    /// Produces the exception used for both the cancellation and timeout branches. Defaults to
    /// <see cref="OperationCanceledException"/> for cancellation and <see cref="TimeoutException"/> for the timeout,
    /// via the defaults of <see cref="PuppeteerExtras.FromCancellationToken"/> and <see cref="PuppeteerExtras.Timeout"/> respectively.
    /// </param>
    /// <param name="retryDelay">The delay between retry attempts. Defaults to 50 milliseconds.</param>
    /// <param name="cancellationToken">A token that, when cancelled, aborts the whole operation immediately.</param>
    /// <returns>An observable that retries <paramref name="source"/> until it succeeds, times out, or is cancelled.</returns>
    public static Observable<T> RetryAndRaceWithSignalAndTimer<T>(
        this Observable<T> source,
        TimeSpan timeout,
        Func<Exception>? causeFactory,
        TimeSpan? retryDelay,
        CancellationToken cancellationToken)
        => source
            .Retry(delay: retryDelay ?? TimeSpan.FromMilliseconds(50))
            .RaceWithSignalAndTimer(timeout, causeFactory, cancellationToken);

    /// <summary>
    /// Overload of <see cref="RetryAndRaceWithSignalAndTimer{T}(Observable{T}, TimeSpan, Func{Exception}, TimeSpan?, CancellationToken)"/>
    /// using the default retry delay (50 milliseconds) and default cause factory (<see cref="OperationCanceledException"/>/<see cref="TimeoutException"/>).
    /// </summary>
    /// <typeparam name="T">The type of values produced by <paramref name="source"/>.</typeparam>
    /// <param name="source">The source sequence to retry.</param>
    /// <param name="timeout">The overall duration before the action is abandoned with a timeout error.</param>
    /// <param name="cancellationToken">A token that, when cancelled, aborts the whole operation immediately.</param>
    /// <returns>An observable that retries <paramref name="source"/> until it succeeds, times out, or is cancelled.</returns>
    public static Observable<T> RetryAndRaceWithSignalAndTimer<T>(this Observable<T> source, TimeSpan timeout, CancellationToken cancellationToken)
        => source.RetryAndRaceWithSignalAndTimer(timeout, causeFactory: null, retryDelay: null, cancellationToken);
}
