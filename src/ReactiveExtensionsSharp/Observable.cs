namespace ReactiveExtensionsSharp;

/// <summary>A push-based sequence of values. Mirrors rxjs's <c>Observable</c>.</summary>
/// <typeparam name="T">The type of the values this observable emits.</typeparam>
public sealed class Observable<T> : IObservable<T>
{
    private readonly Func<Subscriber<T>, IDisposable?> _subscribe;

    /// <summary>
    /// Initializes a new instance of the <see cref="Observable{T}"/> class from a subscribe action that
    /// registers its own teardown logic on the <see cref="Subscriber{T}"/> it receives (via <see cref="Subscription.Add(IDisposable)"/>)
    /// instead of returning a disposable.
    /// </summary>
    /// <param name="subscribe">Invoked once per subscription with the <see cref="Subscriber{T}"/> to push notifications to.</param>
    public Observable(Action<Subscriber<T>> subscribe)
        : this(subscriber =>
        {
            subscribe(subscriber);
            return null;
        })
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="Observable{T}"/> class from a subscribe function that
    /// returns the teardown logic for the subscription (or <see langword="null"/> if none is needed).
    /// </summary>
    /// <param name="subscribe">Invoked once per subscription with the <see cref="Subscriber{T}"/> to push notifications to; returns the disposable to run on unsubscribe, if any.</param>
    public Observable(Func<Subscriber<T>, IDisposable?> subscribe) => _subscribe = subscribe;

    /// <summary>
    /// Subscribes the given observer to this observable, invoking the subscribe delegate passed to the
    /// constructor. If that delegate throws synchronously, the exception is forwarded to <paramref name="observer"/>
    /// via <see cref="IObserver{T}.OnError(Exception)"/> instead of propagating to the caller.
    /// </summary>
    /// <param name="observer">The observer to receive notifications. Wrapped in a <see cref="Subscriber{T}"/> unless it already is one.</param>
    /// <returns>A disposable that unsubscribes <paramref name="observer"/> from this observable.</returns>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        var subscriber = observer as Subscriber<T> ?? new Subscriber<T>(observer);

        try
        {
            var teardown = _subscribe(subscriber);
            if (teardown is not null)
            {
                if (subscriber.IsDisposed)
                {
                    teardown.Dispose();
                }
                else
                {
                    subscriber.Add(teardown);
                }
            }
        }
        catch (Exception ex)
        {
            subscriber.OnError(ex);
        }

        return subscriber;
    }

    /// <summary>
    /// Subscribes to this observable using bare delegates instead of an <see cref="IObserver{T}"/> instance.
    /// Any callback left <see langword="null"/> is simply not invoked for that kind of notification
    /// (an omitted <paramref name="onError"/> falls back to <see cref="RxConfig.OnUnhandledError"/>).
    /// </summary>
    /// <param name="onNext">Invoked for each emitted value.</param>
    /// <param name="onError">Invoked if the source errors.</param>
    /// <param name="onComplete">Invoked if the source completes normally.</param>
    /// <returns>A disposable that unsubscribes from this observable.</returns>
    public IDisposable Subscribe(Action<T>? onNext = null, Action<Exception>? onError = null, Action? onComplete = null)
        => Subscribe(Subscriber.Create(onNext, onError, onComplete));
}
