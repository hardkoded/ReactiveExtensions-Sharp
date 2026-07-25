namespace RxSharp.Subjects;

/// <summary>
/// A <see cref="Subject{T}"/> that only emits its last nexted value, and only upon completion. If it errors
/// instead, no value is ever emitted to any subscriber, regardless of what was nexted beforehand. Mirrors rxjs's
/// <c>AsyncSubject</c>.
/// </summary>
public sealed class AsyncSubject<T> : Subject<T>
{
    private readonly object _gate = new object();
    private T _value = default!;
    private bool _hasValue;

    // Deliberately distinct from the base Subject's IsStopped: this flips to true at the very start of
    // OnCompleted, before any forwarding happens, so a reentrant OnCompleted/Subscribe call (triggered from
    // within an observer callback invoked by our own forwarding) sees it immediately -- IsStopped itself would
    // still read false at that point, since the base Subject only sets it once forwarding is already done.
    private bool _isComplete;

    public override void OnNext(T value)
    {
        if (IsStopped || IsDisposed)
        {
            return;
        }

        lock (_gate)
        {
            _value = value;
            _hasValue = true;
        }
    }

    public override void OnCompleted()
    {
        bool alreadyComplete;
        bool hasValue;
        T value;
        lock (_gate)
        {
            alreadyComplete = _isComplete;
            _isComplete = true;
            hasValue = _hasValue;
            value = _value;
        }

        if (alreadyComplete)
        {
            return;
        }

        if (hasValue)
        {
            base.OnNext(value);
        }

        base.OnCompleted();
    }

    public override IDisposable Subscribe(IObserver<T> observer)
    {
        if (HasError)
        {
            return base.Subscribe(observer);
        }

        bool isComplete;
        bool hasValue;
        T value;
        lock (_gate)
        {
            isComplete = _isComplete;
            hasValue = _hasValue;
            value = _value;
        }

        if (isComplete)
        {
            if (hasValue)
            {
                observer.OnNext(value);
            }

            observer.OnCompleted();
            return Subscription.Empty;
        }

        return base.Subscribe(observer);
    }
}
