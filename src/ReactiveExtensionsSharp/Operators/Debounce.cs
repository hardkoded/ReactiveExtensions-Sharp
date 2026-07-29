namespace ReactiveExtensionsSharp.Operators;

/// <summary>The <c>debounce</c> operator.</summary>
public static class DebounceOperator
{
    /// <summary>
    /// Like <see cref="DebounceTimeOperator.DebounceTime{T}"/>, but the "quiet period" is a per-value inner
    /// "duration" observable (<paramref name="durationSelector"/>) instead of a fixed <see cref="TimeSpan"/>.
    /// Every new source value cancels whatever duration observable is still pending for the previous value and
    /// subscribes a fresh one; the pending value is emitted as soon as that duration observable emits its first
    /// value or completes (an already-scalar/empty/synchronous duration selector therefore lets the value straight
    /// through). Mirrors rxjs's <c>debounce</c>.
    /// </summary>
    /// <typeparam name="T">The element type of the source.</typeparam>
    /// <typeparam name="TDuration">The (unused) element type of the duration observable.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="durationSelector">Given the most recent value, returns the observable whose first emission or completion lets it through.</param>
    /// <returns>An observable that emits the most recent value once the quiet period elapses.</returns>
    public static Observable<T> Debounce<T, TDuration>(this Observable<T> source, Func<T, Observable<TDuration>> durationSelector)
        => source.Operate<T, T>((src, subscriber) =>
        {
            Subscriber<TDuration>? durationSubscriber = null;
            var hasValue = false;
            T lastValue = default!;

            // Disposes and forgets the current per-value duration subscription, if any. Removing it from
            // `subscriber`'s finalizer list (not just disposing it) keeps that list bounded across a long-running
            // stream instead of growing by one stale entry per source value.
            void ClearDuration()
            {
                if (durationSubscriber is null)
                {
                    return;
                }

                subscriber.Remove(durationSubscriber);
                durationSubscriber.Dispose();
                durationSubscriber = null;
            }

            void Emit()
            {
                ClearDuration();
                if (!hasValue)
                {
                    return;
                }

                hasValue = false;
                var value = lastValue;
                lastValue = default!;
                subscriber.OnNext(value);
            }

            return src.SubscribeChild(
                subscriber,
                onNext: value =>
                {
                    Observable<TDuration> duration;
                    try
                    {
                        duration = durationSelector(value);
                    }
                    catch (Exception ex)
                    {
                        subscriber.OnError(ex);
                        return;
                    }

                    ClearDuration();
                    lastValue = value;
                    hasValue = true;

                    // Built directly via Subscriber.Create and assigned/added *before* Subscribe runs (mirroring
                    // WindowWhen/BufferWhen's closing-notifier fix — see CLAUDE.md) rather than a
                    // SingleAssignmentDisposable reassigned only after Subscribe returns: if `duration` emits and
                    // then completes synchronously (e.g. Observable.Of(x)), the reentrant Emit() call from the
                    // first notification disposes this very subscriber, so the guard already built into
                    // Subscriber{T}.OnNext/OnCompleted (checked via IsDisposed) silently no-ops the second,
                    // immediately-following notification instead of double-emitting.
                    var innerSubscriber = Subscriber.Create<TDuration>(
                        onNext: _ => Emit(),
                        onError: subscriber.OnError,
                        onComplete: Emit);
                    durationSubscriber = innerSubscriber;
                    subscriber.Add(innerSubscriber);
                    duration.Subscribe(innerSubscriber);
                },
                onError: subscriber.OnError,
                onComplete: () =>
                {
                    Emit();
                    subscriber.OnCompleted();
                });
        });
}
