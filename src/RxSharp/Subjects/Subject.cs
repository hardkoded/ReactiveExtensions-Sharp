namespace RxSharp.Subjects;

/// <summary>
/// A multicast <see cref="IObservable{T}"/> that is also an <see cref="IObserver{T}"/>: every value/error/completion
/// pushed into it via the <see cref="IObserver{T}"/> methods is forwarded to every observer currently subscribed at
/// that moment. Unlike <see cref="Observable{T}"/>, it is implemented directly against the BCL interfaces rather
/// than by subclassing <see cref="Observable{T}"/> — same shape as rxjs's own <c>Subject</c>.
/// </summary>
/// <typeparam name="T">The type of values pushed through the subject.</typeparam>
public class Subject<T> : IObservable<T>, IObserver<T>, IDisposable
{
    private readonly object _gate = new object();
    private List<IObserver<T>> _observers = new List<IObserver<T>>();
    private bool _isStopped;
    private bool _hasError;
    private Exception? _thrownError;

    /// <summary>Gets a value indicating whether <see cref="Dispose"/> has been called on this subject.</summary>
    public bool IsDisposed { get; private set; }

    /// <summary>
    /// Gets a value indicating whether the subject has already terminated via <see cref="OnError"/> or
    /// <see cref="OnCompleted"/>. Exposed to subclasses (see <see cref="RxSharp.Subjects.ReplaySubject{T}"/>) so they
    /// can consult the terminal state for their own bookkeeping — e.g. to avoid buffering a value that arrives after
    /// termination, even though it is correctly no longer forwarded to subscribers.
    /// </summary>
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

    /// <summary>
    /// Gets a value indicating whether the subject has already terminated via <see cref="OnError"/>. Exposed to
    /// subclasses (see <see cref="RxSharp.Subjects.BehaviorSubject{T}"/>, <see cref="RxSharp.Subjects.AsyncSubject{T}"/>)
    /// so they can consult the error state for their own bookkeeping.
    /// </summary>
    protected bool HasError
    {
        get
        {
            lock (_gate)
            {
                return _hasError;
            }
        }
    }

    /// <summary>Gets the error the subject terminated with, or <see langword="null"/> if it hasn't errored (or was disposed after erroring).</summary>
    protected Exception? ThrownError
    {
        get
        {
            lock (_gate)
            {
                return _thrownError;
            }
        }
    }

    /// <summary>Pushes a value to every observer currently subscribed. A no-op once the subject has stopped or been disposed.</summary>
    /// <param name="value">The value to push.</param>
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

    /// <summary>
    /// Terminates the subject with an error: forwards it to every observer currently subscribed, marks the subject
    /// as stopped, and clears the observer list (any late subscriber will immediately receive the same error — see
    /// <see cref="Subscribe(IObserver{T})"/>). A no-op once the subject has already stopped or been disposed.
    /// </summary>
    /// <param name="error">The terminating error.</param>
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

    /// <summary>
    /// Terminates the subject normally: forwards completion to every observer currently subscribed, marks the
    /// subject as stopped, and clears the observer list (any late subscriber will immediately receive completion
    /// too — see <see cref="Subscribe(IObserver{T})"/>). A no-op once the subject has already stopped or been disposed.
    /// </summary>
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

    /// <summary>
    /// Subscribes <paramref name="observer"/> to future values pushed through this subject. If the subject has
    /// already terminated (<see cref="OnError"/> or <see cref="OnCompleted"/> already called), the observer
    /// immediately receives that same terminal notification instead of being added to the observer list.
    /// </summary>
    /// <param name="observer">The observer to subscribe.</param>
    /// <returns>
    /// A disposable that removes <paramref name="observer"/> from the subject's observer list when disposed, or
    /// <see cref="Subscription.Empty"/> if the subject had already terminated (nothing to unsubscribe from).
    /// </returns>
    /// <exception cref="ObjectDisposedException">The subject has already been disposed.</exception>
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

    /// <summary>
    /// Delegate-based convenience overload of <see cref="Subscribe(IObserver{T})"/>, mirroring the same overload on
    /// <see cref="Observable{T}"/>. Lets a caller pass bare lambdas/method groups directly to a <see cref="Subject{T}"/>
    /// instance without first wrapping it via <see cref="AsObservable"/>, matching what real rxjs subjects support directly.
    /// </summary>
    /// <param name="onNext">Called for each value pushed through the subject. Optional.</param>
    /// <param name="onError">Called once if the subject terminates with an error. Optional.</param>
    /// <param name="onComplete">Called once if the subject terminates normally. Optional.</param>
    /// <returns>A disposable that unsubscribes the constructed observer, per <see cref="Subscribe(IObserver{T})"/>.</returns>
    public IDisposable Subscribe(Action<T>? onNext = null, Action<Exception>? onError = null, Action? onComplete = null)
        => Subscribe(Subscriber.Create(onNext, onError, onComplete));

    /// <summary>Wraps this subject so it is exposed to callers purely as an <see cref="Observable{T}"/>, hiding its <see cref="IObserver{T}"/> (push) side.</summary>
    /// <returns>An <see cref="Observable{T}"/> that subscribes through this subject.</returns>
    // `subscriber => Subscribe(subscriber)` is compatible with both Observable<T> constructor overloads (the
    // Action<Subscriber<T>> one would silently discard the RemovalDisposable Subscribe returns), but C# prefers
    // the Func-returning overload in this exact tie — confirmed by a clean-rebuild test, not just assumed; see
    // CLAUDE.md's Learnings for why this is worth calling out explicitly.
    public Observable<T> AsObservable() => new Observable<T>(subscriber => Subscribe(subscriber));

    /// <summary>Disposes the subject: marks it disposed and drops all current observers without notifying them.</summary>
    public virtual void Dispose()
    {
        lock (_gate)
        {
            IsDisposed = true;
            _observers = new List<IObserver<T>>();
            _thrownError = null;
        }
    }

    /// <summary>Removes a previously subscribed observer from the notification list. Used by the disposable returned from <see cref="Subscribe(IObserver{T})"/>.</summary>
    /// <param name="observer">The observer to remove.</param>
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
