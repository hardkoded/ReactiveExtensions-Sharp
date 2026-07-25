namespace RxSharp;

/// <summary>A disposable that composes child teardown logic. Mirrors rxjs's <c>Subscription</c>.</summary>
public class Subscription : IDisposable
{
    private readonly object _gate = new object();
    private List<IDisposable>? _finalizers;
    private bool _isDisposed;

    /// <summary>Initializes a new instance of the <see cref="Subscription"/> class with no teardown logic yet.</summary>
    public Subscription()
    {
    }

    /// <summary>Initializes a new instance of the <see cref="Subscription"/> class that runs <paramref name="finalizer"/> when disposed.</summary>
    /// <param name="finalizer">The teardown logic to run on <see cref="Dispose"/>.</param>
    public Subscription(Action finalizer)
    {
        _finalizers = new List<IDisposable> { new AnonymousDisposable(finalizer) };
    }

    /// <summary>Gets a shared, already-disposed <see cref="Subscription"/> instance, useful as a no-op return value.</summary>
    public static Subscription Empty { get; } = CreateClosed();

    /// <summary>Gets a value indicating whether this subscription has been disposed (i.e. unsubscribed).</summary>
    public bool IsDisposed
    {
        get
        {
            lock (_gate)
            {
                return _isDisposed;
            }
        }
    }

    /// <summary>
    /// Registers <paramref name="teardown"/> to be disposed when this subscription is disposed. If this
    /// subscription is already disposed, <paramref name="teardown"/> is disposed immediately instead. Passing
    /// this subscription itself is a no-op (avoids a self-referential dispose loop).
    /// </summary>
    /// <param name="teardown">The child disposable to add.</param>
    public void Add(IDisposable teardown)
    {
        if (ReferenceEquals(teardown, this))
        {
            return;
        }

        bool disposeNow;
        lock (_gate)
        {
            disposeNow = _isDisposed;
            if (!disposeNow)
            {
                (_finalizers ??= new List<IDisposable>()).Add(teardown);
            }
        }

        if (disposeNow)
        {
            teardown.Dispose();
        }
    }

    /// <summary>Registers <paramref name="finalizer"/> to run when this subscription is disposed. Equivalent to <see cref="Add(IDisposable)"/> with an anonymous disposable wrapping the action.</summary>
    /// <param name="finalizer">The teardown action to add.</param>
    public void Add(Action finalizer) => Add(new AnonymousDisposable(finalizer));

    /// <summary>Removes a previously added <paramref name="teardown"/> without disposing it, so it will not run when this subscription is later disposed.</summary>
    /// <param name="teardown">The child disposable to remove.</param>
    public void Remove(IDisposable teardown)
    {
        lock (_gate)
        {
            _finalizers?.Remove(teardown);
        }
    }

    /// <summary>Unsubscribes by disposing this subscription. Equivalent to <see cref="Dispose"/>; mirrors rxjs's <c>unsubscribe()</c> naming.</summary>
    public void Unsubscribe() => Dispose();

    /// <summary>
    /// Disposes every child teardown added via <see cref="Add(IDisposable)"/>, in the order they were added.
    /// Safe to call more than once — subsequent calls are no-ops. If one or more finalizers throw, disposal of
    /// the remaining finalizers still proceeds, and any collected exceptions are re-thrown together, wrapped in
    /// an <see cref="UnsubscriptionException"/>.
    /// </summary>
    public void Dispose()
    {
        List<IDisposable>? finalizers;
        lock (_gate)
        {
            if (_isDisposed)
            {
                return;
            }

            _isDisposed = true;
            finalizers = _finalizers;
            _finalizers = null;
        }

        if (finalizers is null)
        {
            return;
        }

        List<Exception>? errors = null;
        foreach (var finalizer in finalizers)
        {
            try
            {
                finalizer.Dispose();
            }
            catch (Exception ex)
            {
                (errors ??= new List<Exception>()).Add(ex);
            }
        }

        if (errors is { Count: > 0 })
        {
            throw new UnsubscriptionException(errors);
        }
    }

    private static Subscription CreateClosed()
    {
        var subscription = new Subscription();
        subscription.Dispose();
        return subscription;
    }

    private sealed class AnonymousDisposable : IDisposable
    {
        private Action? _action;

        public AnonymousDisposable(Action action) => _action = action;

        public void Dispose() => Interlocked.Exchange(ref _action, null)?.Invoke();
    }
}
