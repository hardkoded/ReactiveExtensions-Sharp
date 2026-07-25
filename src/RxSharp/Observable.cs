namespace RxSharp;

/// <summary>A push-based sequence of values. Mirrors rxjs's <c>Observable</c>.</summary>
public sealed class Observable<T> : IObservable<T>
{
    private readonly Func<Subscriber<T>, IDisposable?> _subscribe;

    public Observable(Action<Subscriber<T>> subscribe)
        : this(subscriber =>
        {
            subscribe(subscriber);
            return null;
        })
    {
    }

    public Observable(Func<Subscriber<T>, IDisposable?> subscribe) => _subscribe = subscribe;

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

    public IDisposable Subscribe(Action<T>? onNext = null, Action<Exception>? onError = null, Action? onComplete = null)
        => Subscribe(Subscriber.Create(onNext, onError, onComplete));
}
