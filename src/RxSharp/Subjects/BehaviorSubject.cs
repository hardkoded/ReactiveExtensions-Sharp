namespace RxSharp.Subjects;

/// <summary>
/// A <see cref="Subject{T}"/> that requires an initial value and synchronously replays its current value to every
/// new subscriber. Mirrors rxjs's <c>BehaviorSubject</c>.
/// </summary>
/// <typeparam name="T">The type of values pushed through the subject.</typeparam>
public class BehaviorSubject<T> : Subject<T>
{
    private readonly object _valueGate = new object();
    private T _value;

    /// <summary>Initializes a new instance of the <see cref="BehaviorSubject{T}"/> class with the given current value.</summary>
    /// <param name="value">The initial current value, returned by <see cref="Value"/> and replayed to the first subscriber(s) until the next <see cref="OnNext"/>.</param>
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

    /// <summary>Pushes a new current value and forwards it to every observer currently subscribed. A no-op forward once the subject has stopped or been disposed (see <see cref="Subject{T}.OnNext"/>).</summary>
    /// <param name="value">The new current value.</param>
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

    /// <summary>Subscribes <paramref name="observer"/> and, unless the subject had already terminated, immediately replays the current value to it.</summary>
    /// <param name="observer">The observer to subscribe.</param>
    /// <returns>A disposable that unsubscribes <paramref name="observer"/>, per <see cref="Subject{T}.Subscribe(IObserver{T})"/>.</returns>
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
