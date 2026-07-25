using RxSharp.Subjects;

namespace RxSharp.Operators;

/// <summary>Extension methods implementing the <c>retryWhen</c> operator.</summary>
/// <remarks>
/// A more general, config-driven version of <see cref="RetryOperator.Retry{T}"/>'s fixed count/delay: instead
/// of a retry budget, <c>notifier</c> receives an observable of every error <c>source</c> raises and returns an
/// observable whose emissions each trigger a resubscription. If the notifier observable completes, the whole
/// sequence completes (without ever forwarding the error that triggered the notifier); if it errors, that error
/// becomes the final error forwarded downstream.
/// </remarks>
public static class RetryWhenOperator
{
    /// <summary>
    /// Resubscribes to <paramref name="source"/> whenever it errors, but only once the observable returned by
    /// <paramref name="notifier"/> (given an observable of every error so far) emits a value in response to
    /// that error. If the notifier observable completes instead of emitting, the sequence completes normally.
    /// If it errors, that error is forwarded downstream via <c>OnError</c> instead of the original.
    /// </summary>
    /// <remarks>
    /// <paramref name="notifier"/> is called once per subscription, lazily, the first time <paramref name="source"/>
    /// errors — not eagerly at subscribe time. If <paramref name="notifier"/> itself throws, that exception is
    /// forwarded via <c>OnError</c>. The value type of the notifier's output observable is never inspected —
    /// only whether and when it emits, errors, or completes.
    /// </remarks>
    /// <typeparam name="T">The type of values emitted by <paramref name="source"/>.</typeparam>
    /// <typeparam name="TNotification">The element type of the observable returned by <paramref name="notifier"/>; only its emissions/termination matter, not the values themselves.</typeparam>
    /// <param name="source">The source sequence to retry on error.</param>
    /// <param name="notifier">
    /// A function, called with an observable of every error <paramref name="source"/> has raised so far,
    /// that returns an observable whose emissions each trigger a resubscription to <paramref name="source"/>.
    /// </param>
    /// <returns>An observable that mirrors <paramref name="source"/>, retrying on error as directed by <paramref name="notifier"/>.</returns>
    public static Observable<T> RetryWhen<T, TNotification>(this Observable<T> source, Func<Observable<Exception>, Observable<TNotification>> notifier)
        => source.Operate<T, T>((src, subscriber) =>
        {
            var sourceSubscription = new SingleAssignmentDisposable();
            Subject<Exception>? errors = null;

            void SubscribeForRetry()
            {
                // Guards against resubscribing once downstream has already gone away — e.g. a fully
                // synchronous, immediately-erroring source paired with a notifier that emits synchronously:
                // each resubscription's own error handling is independent of the shared downstream subscriber's
                // state (a fresh per-attempt subscriber is created on every call to Subscribe), so without this
                // check a source/notifier pair that keeps completing synchronously would keep recursing
                // forever (eventually a StackOverflowException) even after downstream stopped caring.
                if (subscriber.IsDisposed)
                {
                    return;
                }

                // Built directly (see Take.cs for the full explanation) rather than via the Subscribe(onNext:...)
                // convenience overload, so the error handler can dispose *this exact, already-live* per-attempt
                // subscriber itself before triggering the next attempt. That matters because an error, for a
                // fully synchronous source, dispatches to this callback *before* Subscribe ever returns — so any
                // teardown the source registered on this subscriber (see Observable's Action<Subscriber<T>>
                // constructor) needs to run here, not after the whole recursive chain of future attempts has
                // already unwound, to match rxjs's "always finalize before the next attempt" contract.
                Subscriber<T> attemptSubscriber = null!;
                attemptSubscriber = Subscriber.Create<T>(
                    onNext: subscriber.OnNext,
                    onError: err =>
                    {
                        attemptSubscriber.Dispose();

                        if (subscriber.IsDisposed)
                        {
                            return;
                        }

                        if (errors is null)
                        {
                            var errorsSubject = new Subject<Exception>();
                            errors = errorsSubject;

                            Observable<TNotification> notifierObservable;
                            try
                            {
                                notifierObservable = notifier(errorsSubject.AsObservable());
                            }
                            catch (Exception ex)
                            {
                                subscriber.OnError(ex);
                                return;
                            }

                            notifierObservable.Subscribe(
                                onNext: _ => SubscribeForRetry(),
                                onError: subscriber.OnError,
                                onComplete: subscriber.OnCompleted);
                        }

                        errors.OnNext(err);
                    },
                    onComplete: subscriber.OnCompleted);

                sourceSubscription.Disposable = src.Subscribe(attemptSubscriber);
            }

            SubscribeForRetry();
            return sourceSubscription;
        });
}
