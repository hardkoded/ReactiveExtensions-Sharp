using ReactiveExtensionsSharp.Subjects;

namespace ReactiveExtensionsSharp.Operators;

/// <summary>Extension methods implementing the <c>repeatWhen</c> operator.</summary>
/// <remarks>
/// The <see cref="RepeatOperator.Repeat{T}"/> counterpart to <see cref="RetryWhenOperator.RetryWhen{T, TNotification}"/>:
/// instead of a repeat budget, <c>notifier</c> receives an observable of a notification for every completion
/// <c>source</c> raises and returns an observable whose emissions each trigger a resubscription. If the notifier
/// observable completes, the whole sequence completes; if it errors, that error becomes the final error
/// forwarded downstream. Unlike <see cref="RetryWhenOperator.RetryWhen{T, TNotification}"/>, <c>source</c> errors
/// are never intercepted here — they are forwarded via <c>OnError</c> immediately, matching
/// <see cref="RepeatOperator.Repeat{T}"/>'s own "retry is for errors, repeat is for completions" split.
/// </remarks>
public static class RepeatWhenOperator
{
    /// <summary>
    /// Resubscribes to <paramref name="source"/> whenever it completes, but only once the observable returned by
    /// <paramref name="notifier"/> (given an observable of one notification per completion so far) emits a value
    /// in response to that completion. If the notifier observable completes instead of emitting, the sequence
    /// completes. If it errors, that error is forwarded downstream via <c>OnError</c>.
    /// </summary>
    /// <remarks>
    /// <paramref name="notifier"/> is called once per subscription, lazily, the first time <paramref name="source"/>
    /// completes — not eagerly at subscribe time. If <paramref name="notifier"/> itself throws, that exception is
    /// forwarded via <c>OnError</c>. The value type of the notifier's output observable is never inspected —
    /// only whether and when it emits, errors, or completes. Rxjs passes <c>Observable&lt;void&gt;</c> for the
    /// notification channel; this port uses <see cref="Unit"/> in its place (see CLAUDE.md's Core design).
    /// </remarks>
    /// <typeparam name="T">The type of values emitted by <paramref name="source"/>.</typeparam>
    /// <typeparam name="TNotification">The element type of the observable returned by <paramref name="notifier"/>; only its emissions/termination matter, not the values themselves.</typeparam>
    /// <param name="source">The source sequence to repeat on completion.</param>
    /// <param name="notifier">
    /// A function, called with an observable of one notification per completion <paramref name="source"/> has
    /// raised so far, that returns an observable whose emissions each trigger a resubscription to <paramref name="source"/>.
    /// </param>
    /// <returns>An observable that mirrors <paramref name="source"/>, resubscribing on completion as directed by <paramref name="notifier"/>.</returns>
    public static Observable<T> RepeatWhen<T, TNotification>(this Observable<T> source, Func<Observable<Unit>, Observable<TNotification>> notifier)
        => source.Operate<T, T>((src, subscriber) =>
        {
            Subject<Unit>? completions = null;

            void SubscribeForRepeat()
            {
                // Guards against recursing into another cycle once downstream has already gone away — same
                // stack-overflow-avoidance reasoning as Repeat/RetryWhen (see CLAUDE.md): a rapidly,
                // synchronously completing source paired with a notifier that emits synchronously would
                // otherwise keep recursing forever even after downstream stopped caring.
                if (subscriber.IsDisposed)
                {
                    return;
                }

                // Built directly and registered as a child of `subscriber` *before* subscribing (see
                // OperatorHelper.SubscribeChild's doc comment for the general pattern), rather than via the
                // Subscribe(onNext:...) convenience overload, for two reasons: (1) the completion handler can
                // dispose *this exact, already-live* per-cycle subscriber itself before triggering the next
                // cycle, matching rxjs's "always finalize before the next cycle" contract even for a fully
                // synchronous source; (2) a new cycleSubscriber replaces the previous one on every completion,
                // so it must also be Remove()'d once the cycle ends to avoid the downstream subscriber's
                // finalizer list growing unboundedly across many cycles.
                Subscriber<T> cycleSubscriber = null!;
                cycleSubscriber = Subscriber.Create<T>(
                    onNext: subscriber.OnNext,
                    onError: subscriber.OnError,
                    onComplete: () =>
                    {
                        subscriber.Remove(cycleSubscriber);
                        cycleSubscriber.Dispose();

                        if (subscriber.IsDisposed)
                        {
                            return;
                        }

                        if (completions is null)
                        {
                            var completionsSubject = new Subject<Unit>();
                            completions = completionsSubject;

                            Observable<TNotification> notifierObservable;
                            try
                            {
                                notifierObservable = notifier(completionsSubject.AsObservable());
                            }
                            catch (Exception ex)
                            {
                                subscriber.OnError(ex);
                                return;
                            }

                            // A single stable subscription for this operator's whole lifetime (only ever
                            // created once, memoized above) — exactly what SubscribeChild targets, so a
                            // downstream disposal (external unsubscribe, or a further-downstream operator like
                            // Take completing early) correctly cascades into tearing this down too, instead of
                            // leaving it listening forever.
                            notifierObservable.SubscribeChild(
                                subscriber,
                                onNext: _ => SubscribeForRepeat(),
                                onError: subscriber.OnError,
                                onComplete: subscriber.OnCompleted);
                        }

                        completions.OnNext(Unit.Default);
                    });

                subscriber.Add(cycleSubscriber);
                src.Subscribe(cycleSubscriber);
            }

            SubscribeForRepeat();
            return null;
        });
}
