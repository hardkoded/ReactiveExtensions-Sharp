namespace ReactiveExtensionsSharp.Extras;

/// <summary>Puppeteer-flavored combinators built on top of the core primitives — the C# analogues of the helpers Puppeteer itself layers on top of rxjs (see CLAUDE.md's "Puppeteer-essential surface").</summary>
public static partial class RxExtensions
{
    /// <summary>
    /// An observable that never emits and errors as soon as <paramref name="cancellationToken"/> is cancelled.
    /// Mirrors Puppeteer's <c>fromAbortSignal</c>, completing the shape that helper fills in JS on top of rxjs.
    /// If the token is already cancelled at subscribe time, <see cref="IObserver{T}.OnError"/> fires synchronously
    /// during subscription rather than waiting for a cancellation callback.
    /// </summary>
    /// <param name="cancellationToken">The token whose cancellation should be surfaced as an error.</param>
    /// <param name="causeFactory">
    /// Produces the exception passed to <see cref="IObserver{T}.OnError"/> when the token is cancelled. Defaults to
    /// a new <see cref="OperationCanceledException"/>.
    /// </param>
    /// <returns>An observable that never calls <see cref="IObserver{T}.OnNext"/> and only ever errors.</returns>
    public static Observable<Unit> FromCancellationToken(CancellationToken cancellationToken, Func<Exception>? causeFactory = null)
        => new Observable<Unit>(subscriber =>
        {
            var makeCause = causeFactory ?? DefaultCancellationCause;

            if (cancellationToken.IsCancellationRequested)
            {
                subscriber.OnError(makeCause());
                return null;
            }

            var registration = cancellationToken.Register(() => subscriber.OnError(makeCause()));
            return new Subscription(() => registration.Dispose());
        });

    private static Exception DefaultCancellationCause() => new OperationCanceledException();
}
