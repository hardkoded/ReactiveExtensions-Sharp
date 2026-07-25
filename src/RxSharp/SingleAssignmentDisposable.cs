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
