namespace RxSharp;

/// <summary>
/// An <see cref="IDisposable"/> whose target is assigned after construction. Disposing before the
/// target is assigned disposes that target as soon as it is set. Needed by operators (e.g. <c>Take</c>)
/// that must be able to unsubscribe from a source before that source's own synchronous <c>Subscribe</c>
/// call has returned — e.g. when the first emitted value already satisfies the operator.
/// </summary>
public sealed class SingleAssignmentDisposable : IDisposable
{
    private readonly object _gate = new object();
    private IDisposable? _disposable;
    private bool _isDisposed;

    /// <summary>
    /// Gets or sets the disposable this instance wraps. Setting this before <see cref="Dispose"/> has been called
    /// simply reassigns the wrapped target (the previous target, if any, is <em>not</em> disposed — see the type
    /// summary). Setting it after <see cref="Dispose"/> has been called disposes the new value immediately instead
    /// of storing it.
    /// </summary>
    public IDisposable? Disposable
    {
        get
        {
            lock (_gate)
            {
                return _disposable;
            }
        }

        set
        {
            IDisposable? toDispose = null;
            lock (_gate)
            {
                if (_isDisposed)
                {
                    toDispose = value;
                }
                else
                {
                    _disposable = value;
                }
            }

            toDispose?.Dispose();
        }
    }

    /// <summary>Disposes the currently-wrapped <see cref="Disposable"/>, if any, and marks this instance as disposed. Safe to call more than once — subsequent calls are no-ops.</summary>
    public void Dispose()
    {
        IDisposable? toDispose = null;
        lock (_gate)
        {
            if (!_isDisposed)
            {
                _isDisposed = true;
                toDispose = _disposable;
                _disposable = null;
            }
        }

        toDispose?.Dispose();
    }
}
