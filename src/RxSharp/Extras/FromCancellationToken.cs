namespace RxSharp.Extras;

/// <summary>Puppeteer-flavored combinators built on top of the core primitives — the C# analogues of the helpers Puppeteer itself layers on top of rxjs (see CLAUDE.md's "Puppeteer-essential surface").</summary>
public static class CancellationExtras
{
    /// <summary>An observable that never emits and errors as soon as <paramref name="cancellationToken"/> is cancelled. Mirrors Puppeteer's <c>fromAbortSignal</c>.</summary>
    public static Observable<Unit> FromCancellationToken(CancellationToken cancellationToken, Func<Exception>? causeFactory = null)
        => new Observable<Unit>(subscriber =>
        {
            var makeCause = causeFactory ?? DefaultCause;

            if (cancellationToken.IsCancellationRequested)
            {
                subscriber.OnError(makeCause());
                return null;
            }

            var registration = cancellationToken.Register(() => subscriber.OnError(makeCause()));
            return new Subscription(() => registration.Dispose());
        });

    private static Exception DefaultCause() => new OperationCanceledException();
}
