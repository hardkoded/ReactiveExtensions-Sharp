namespace RxSharp.Subjects;

/// <summary>
/// A <see cref="Subject{T}"/> that requires an initial value and synchronously replays its current value to every
/// new subscriber. Mirrors rxjs's <c>BehaviorSubject</c>.
/// </summary>
public class BehaviorSubject<T> : Subject<T>
{
    private readonly object _valueGate = new object();
    private T _value;

    public BehaviorSubject(T value) => _value = value;

    /// <summary>
    /// The current value. Throws the subject's stored error if it has errored, or
    /// <see cref="ObjectDisposedException"/> if the subject has been disposed. A subject that merely completed
    /// (without erroring) still returns its last value -- matches rxjs's <c>getValue()</c>/<c>value</c> semantics,
    /// where only <c>hasError</c> or "closed" (unsubscribed) throw.
    /// </summary>
    public T Value
    {
        get
        {
            if (HasError)
            {
                throw ThrownError ?? new ObjectDisposedException(nameof(BehaviorSubject<T>));
            }

            if (IsDisposed)
            {
                throw new ObjectDisposedException(nameof(BehaviorSubject<T>));
            }

            lock (_valueGate)
            {
                return _value;
            }
        }
    }

    public override void OnNext(T value)
    {
        // Only track the value while still live: rxjs's own BehaviorSubject technically keeps mutating its
        // internal `_value` field even after being stopped (an artifact of `next()`'s argument-evaluation order
        // in the original source), but no spec test depends on that surprising behavior, so this port only
        // tracks the value while the subject can still meaningfully forward it.
        if (!IsStopped && !IsDisposed)
        {
            lock (_valueGate)
            {
                _value = value;
            }
        }

        base.OnNext(value);
    }

    public override IDisposable Subscribe(IObserver<T> observer)
    {
        var subscription = base.Subscribe(observer);

        // base.Subscribe returns the shared Subscription.Empty sentinel exactly when the subject was already
        // stopped (errored/completed) and already delivered OnError/OnCompleted synchronously -- in that case
        // there's no current value left to replay.
        if (!ReferenceEquals(subscription, Subscription.Empty))
        {
            T current;
            lock (_valueGate)
            {
                current = _value;
            }

            observer.OnNext(current);
        }

        return subscription;
    }
}
