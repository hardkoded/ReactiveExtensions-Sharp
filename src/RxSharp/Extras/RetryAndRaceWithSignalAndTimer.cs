using RxSharp.Operators;

namespace RxSharp.Extras;

public static class RetryAndRaceWithSignalAndTimerExtras
{
    /// <summary>
    /// The combinator behind Puppeteer's <c>Locator</c> actions (click/fill/hover/wait): retry the source
    /// on error, racing the whole thing against cancellation and a timeout. Mirrors Puppeteer's
    /// <c>retryAndRaceWithSignalAndTimer</c>: <c>pipe(retry({delay}), raceWith(fromAbortSignal(...), timeout(...)))</c>.
    /// </summary>
    public static Observable<T> RetryAndRaceWithSignalAndTimer<T>(
        this Observable<T> source,
        TimeSpan timeout,
        Func<Exception>? causeFactory,
        TimeSpan? retryDelay,
        CancellationToken cancellationToken)
        => source
            .Retry(delay: retryDelay ?? TimeSpan.FromMilliseconds(50))
            .RaceWith(
                CancellationExtras.FromCancellationToken(cancellationToken, causeFactory).Map<Unit, T>(NeverReached<T>),
                TimeoutExtras.Timeout(timeout, causeFactory).Map<Unit, T>(NeverReached<T>));

    public static Observable<T> RetryAndRaceWithSignalAndTimer<T>(this Observable<T> source, TimeSpan timeout, CancellationToken cancellationToken)
        => source.RetryAndRaceWithSignalAndTimer(timeout, causeFactory: null, retryDelay: null, cancellationToken);

    // FromCancellationToken/Timeout only ever call OnError, never OnNext — this projection exists purely so
    // the two Observable<Unit> notifiers type-check inside RaceWith(Observable<T>), matching how rxjs relies
    // on TypeScript accepting Observable<never> anywhere an Observable<T> is expected. C# has no bottom type.
    private static TResult NeverReached<TResult>(Unit value) => throw new InvalidOperationException("unreachable: this observable only ever errors");
}
