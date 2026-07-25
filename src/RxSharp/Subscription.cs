namespace RxSharp;

/// <summary>A disposable that composes child teardown logic. Mirrors rxjs's <c>Subscription</c>.</summary>
public class Subscription : IDisposable
{
    private readonly object _gate = new object();
    private List<IDisposable>? _finalizers;
    private bool _isDisposed;

    public Subscription()
    {
    }

    public Subscription(Action finalizer)
    {
        _finalizers = new List<IDisposable> { new AnonymousDisposable(finalizer) };
    }

    public static Subscription Empty { get; } = CreateClosed();

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

    public void Add(Action finalizer) => Add(new AnonymousDisposable(finalizer));

    public void Remove(IDisposable teardown)
    {
        lock (_gate)
        {
            _finalizers?.Remove(teardown);
        }
    }

    public void Unsubscribe() => Dispose();

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
