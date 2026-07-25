namespace RxSharp;

/// <summary>
/// Wraps an <see cref="IObserver{T}"/> with unsubscribe-aware, re-entrancy-guarded dispatch. Mirrors rxjs's <c>Subscriber</c>.
/// Deliberately does not swallow exceptions thrown by the wrapped observer's callbacks: it is each operator's own
/// responsibility to catch exceptions from user-supplied callbacks (projections, predicates, side effects) and
/// forward them via <see cref="OnError"/> — see <c>RxSharp/Operators</c> for the pattern. A generic catch-and-forward
/// here would double up across nested operator chains and silently swallow errors instead of propagating them.
/// </summary>
/// <typeparam name="T">The type of the values this subscriber accepts.</typeparam>
public class Subscriber<T> : Subscription, IObserver<T>
{
    private readonly IObserver<T> _observer;
    private bool _isStopped;

    /// <summary>Initializes a new instance of the <see cref="Subscriber{T}"/> class wrapping the given observer.</summary>
    /// <param name="observer">The observer notifications are forwarded to.</param>
    public Subscriber(IObserver<T> observer) => _observer = observer;

    /// <summary>Forwards <paramref name="value"/> to the wrapped observer, unless this subscriber has already stopped (via error/completion) or been disposed, in which case the call is silently ignored.</summary>
    /// <param name="value">The value to push.</param>
    public void OnNext(T value)
    {
        if (_isStopped || IsDisposed)
        {
            return;
        }

        _observer.OnNext(value);
    }

    /// <summary>
    /// Forwards <paramref name="error"/> to the wrapped observer, then unsubscribes. A no-op if this subscriber
    /// has already stopped or been disposed. After this call, further <see cref="OnNext"/>/<see cref="OnError"/>/<see cref="OnCompleted"/>
    /// calls are ignored.
    /// </summary>
    /// <param name="error">The error to forward.</param>
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

    /// <summary>
    /// Notifies the wrapped observer of completion, then unsubscribes. A no-op if this subscriber has already
    /// stopped or been disposed. After this call, further <see cref="OnNext"/>/<see cref="OnError"/>/<see cref="OnCompleted"/>
    /// calls are ignored.
    /// </summary>
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
    /// <summary>Creates a <see cref="Subscriber{T}"/> from bare delegates instead of an <see cref="IObserver{T}"/> instance. Any callback left <see langword="null"/> is simply not invoked for that notification kind.</summary>
    /// <typeparam name="T">The type of values the subscriber accepts.</typeparam>
    /// <param name="onNext">Invoked for each value, if provided.</param>
    /// <param name="onError">Invoked on error, if provided; otherwise the error is forwarded to <see cref="RxConfig.OnUnhandledError"/>.</param>
    /// <param name="onComplete">Invoked on completion, if provided.</param>
    /// <returns>A new <see cref="Subscriber{T}"/> wrapping the given delegates.</returns>
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
