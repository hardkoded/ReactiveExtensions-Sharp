namespace RxSharp.Subjects;

/// <summary>A multicast <see cref="Observable{T}"/> that is also an <see cref="IObserver{T}"/>. Mirrors rxjs's <c>Subject</c>.</summary>
public class Subject<T> : IObservable<T>, IObserver<T>, IDisposable
{
    private readonly object _gate = new object();
    private List<IObserver<T>> _observers = new List<IObserver<T>>();
    private bool _isStopped;
    private bool _hasError;
    private Exception? _thrownError;

    public bool IsDisposed { get; private set; }

    protected bool IsStopped
    {
        get
        {
            lock (_gate)
            {
                return _isStopped;
            }
        }
    }

    public virtual void OnNext(T value)
    {
        IObserver<T>[] observers;
        lock (_gate)
        {
            if (_isStopped || IsDisposed)
            {
                return;
            }

            observers = _observers.ToArray();
        }

        foreach (var observer in observers)
        {
            observer.OnNext(value);
        }
    }

    public virtual void OnError(Exception error)
    {
        IObserver<T>[] observers;
        lock (_gate)
        {
            if (_isStopped || IsDisposed)
            {
                return;
            }

            _isStopped = true;
            _hasError = true;
            _thrownError = error;
            observers = _observers.ToArray();
            _observers = new List<IObserver<T>>();
        }

        foreach (var observer in observers)
        {
            observer.OnError(error);
        }
    }

    public virtual void OnCompleted()
    {
        IObserver<T>[] observers;
        lock (_gate)
        {
            if (_isStopped || IsDisposed)
            {
                return;
            }

            _isStopped = true;
            observers = _observers.ToArray();
            _observers = new List<IObserver<T>>();
        }

        foreach (var observer in observers)
        {
            observer.OnCompleted();
        }
    }

    public virtual IDisposable Subscribe(IObserver<T> observer)
    {
        lock (_gate)
        {
            CheckDisposed();

            if (_hasError)
            {
                observer.OnError(_thrownError!);
                return Subscription.Empty;
            }

            if (_isStopped)
            {
                observer.OnCompleted();
                return Subscription.Empty;
            }

            _observers.Add(observer);
            return new RemovalDisposable(this, observer);
        }
    }

    public IDisposable Subscribe(Action<T>? onNext = null, Action<Exception>? onError = null, Action? onComplete = null)
        => Subscribe(Subscriber.Create(onNext, onError, onComplete));

    public Observable<T> AsObservable() => new Observable<T>(subscriber => Subscribe(subscriber));

    public virtual void Dispose()
    {
        lock (_gate)
        {
            IsDisposed = true;
            _observers = new List<IObserver<T>>();
            _thrownError = null;
        }
    }

    protected void RemoveObserver(IObserver<T> observer)
    {
        lock (_gate)
        {
            _observers.Remove(observer);
        }
    }

    private void CheckDisposed()
    {
        if (IsDisposed)
        {
            throw new ObjectDisposedException(nameof(Subject<T>));
        }
    }

    private sealed class RemovalDisposable : IDisposable
    {
        private readonly Subject<T> _subject;
        private readonly IObserver<T> _observer;
        private bool _disposed;

        public RemovalDisposable(Subject<T> subject, IObserver<T> observer)
        {
            _subject = subject;
            _observer = observer;
        }

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            _subject.RemoveObserver(_observer);
        }
    }
}
