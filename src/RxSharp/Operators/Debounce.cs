namespace RxSharp.Operators;

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
    public static Observable<T> Debounce<T, TDuration>(this Observable<T> source, Func<T, Observable<TDuration>> durationSelector)
        => source.Operate<T, T>((src, subscriber) =>
        {
            var durationSubscription = new SingleAssignmentDisposable();
            subscriber.Add(durationSubscription);
            var hasValue = false;
            T lastValue = default!;

            void Emit()
            {
                durationSubscription.Disposable?.Dispose();
                durationSubscription.Disposable = null;
                if (!hasValue)
                {
                    return;
                }

                hasValue = false;
                var value = lastValue;
                lastValue = default!;
                subscriber.OnNext(value);
            }

            return src.Subscribe(
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

                    durationSubscription.Disposable?.Dispose();
                    lastValue = value;
                    hasValue = true;
                    durationSubscription.Disposable = duration.Subscribe(
                        onNext: _ => Emit(),
                        onError: subscriber.OnError,
                        onComplete: Emit);
                },
                onError: subscriber.OnError,
                onComplete: () =>
                {
                    Emit();
                    subscriber.OnCompleted();
                });
        });
}
