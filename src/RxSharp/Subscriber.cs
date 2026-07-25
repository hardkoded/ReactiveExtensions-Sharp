namespace RxSharp;

/// <summary>
/// Wraps an <see cref="IObserver{T}"/> with unsubscribe-aware, re-entrancy-guarded dispatch. Mirrors rxjs's <c>Subscriber</c>.
/// Deliberately does not swallow exceptions thrown by the wrapped observer's callbacks: it is each operator's own
/// responsibility to catch exceptions from user-supplied callbacks (projections, predicates, side effects) and
/// forward them via <see cref="OnError"/> — see <c>RxSharp/Operators</c> for the pattern. A generic catch-and-forward
/// here would double up across nested operator chains and silently swallow errors instead of propagating them.
/// </summary>
public class Subscriber<T> : Subscription, IObserver<T>
{
    private readonly IObserver<T> _observer;
    private bool _isStopped;

    public Subscriber(IObserver<T> observer) => _observer = observer;

    public void OnNext(T value)
    {
        if (_isStopped || IsDisposed)
        {
            return;
        }

        _observer.OnNext(value);
    }

    public void OnError(Exception error)
    {
        if (_isStopped || IsDisposed)
        {
            return;
        }

        _isStopped = true;
        try
        {
            _observer.OnError(error);
        }
        finally
        {
            Unsubscribe();
        }
    }

    public void OnCompleted()
    {
        if (_isStopped || IsDisposed)
        {
            return;
        }

        _isStopped = true;
        try
        {
            _observer.OnCompleted();
        }
        finally
        {
            Unsubscribe();
        }
    }
}

/// <summary>Factory helpers for <see cref="Subscriber{T}"/>. A non-generic sibling avoids CA1000 (static members on generic types).</summary>
public static class Subscriber
{
    public static Subscriber<T> Create<T>(Action<T>? onNext = null, Action<Exception>? onError = null, Action? onComplete = null)
        => new Subscriber<T>(new DelegateObserver<T>(onNext, onError, onComplete));

    private sealed class DelegateObserver<T> : IObserver<T>
    {
        private readonly Action<T>? _onNext;
        private readonly Action<Exception>? _onError;
        private readonly Action? _onComplete;

        public DelegateObserver(Action<T>? onNext, Action<Exception>? onError, Action? onComplete)
        {
            _onNext = onNext;
            _onError = onError;
            _onComplete = onComplete;
        }

        public void OnNext(T value) => _onNext?.Invoke(value);

        public void OnError(Exception error)
        {
            if (_onError is not null)
            {
                _onError(error);
            }
            else
            {
                RxConfig.OnUnhandledError(error);
            }
        }

        public void OnCompleted() => _onComplete?.Invoke();
    }
}
