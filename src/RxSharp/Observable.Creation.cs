namespace RxSharp;

/// <summary>Creation functions for <see cref="Observable{T}"/>. Mirrors rxjs's <c>observable/</c> creation functions.</summary>
public static class Observable
{
    public static Observable<T> Of<T>(params T[] values) => From(values);

    public static Observable<T> From<T>(IEnumerable<T> values) => new Observable<T>(subscriber =>
    {
        foreach (var value in values)
        {
            if (subscriber.IsDisposed)
            {
                return;
            }

            subscriber.OnNext(value);
        }

        subscriber.OnCompleted();
    });

    public static Observable<T> From<T>(Task<T> task) => new Observable<T>(subscriber =>
    {
        task.ContinueWith(
            completed =>
            {
                if (subscriber.IsDisposed)
                {
                    return;
                }

                if (completed.IsFaulted)
                {
                    subscriber.OnError(completed.Exception!.GetBaseException());
                }
                else if (completed.IsCanceled)
                {
                    subscriber.OnError(new TaskCanceledException(completed));
                }
                else
                {
                    subscriber.OnNext(completed.Result);
                    subscriber.OnCompleted();
                }
            },
            TaskScheduler.Default);
    });

    public static Observable<T> Defer<T>(Func<Observable<T>> factory) => new Observable<T>(subscriber => factory().Subscribe(subscriber));

    public static Observable<T> Empty<T>() => new Observable<T>(subscriber => subscriber.OnCompleted());

    public static Observable<T> Never<T>() => new Observable<T>(_ => { });

    public static Observable<T> ThrowError<T>(Func<Exception> errorFactory) => new Observable<T>(subscriber => subscriber.OnError(errorFactory()));

    public static Observable<long> Timer(TimeSpan dueTime, IScheduler? scheduler = null) => new Observable<long>(subscriber =>
    {
        var activeScheduler = scheduler ?? TaskPoolScheduler.Instance;
        return activeScheduler.Schedule(
            () =>
            {
                subscriber.OnNext(0L);
                subscriber.OnCompleted();
            },
            dueTime);
    });

    public static Observable<T> Race<T>(params Observable<T>[] sources) => new Observable<T>(subscriber => RaceCore.Subscribe(sources, subscriber));

    public static Observable<T> Concat<T>(params Observable<T>[] sources) => new Observable<T>(subscriber =>
    {
        var index = 0;

        void SubscribeNext()
        {
            if (index >= sources.Length)
            {
                subscriber.OnCompleted();
                return;
            }

            var next = sources[index++];
            var subscription = new SingleAssignmentDisposable();
            subscriber.Add(subscription);
            subscription.Disposable = next.Subscribe(onNext: subscriber.OnNext, onError: subscriber.OnError, onComplete: SubscribeNext);
        }

        SubscribeNext();
    });

    public static Observable<T> Merge<T>(params Observable<T>[] sources) => new Observable<T>(subscriber =>
    {
        if (sources.Length == 0)
        {
            subscriber.OnCompleted();
            return null;
        }

        var remaining = sources.Length;
        var subscriptions = new List<IDisposable>();

        foreach (var source in sources)
        {
            if (subscriber.IsDisposed)
            {
                break;
            }

            subscriptions.Add(source.Subscribe(
                onNext: subscriber.OnNext,
                onError: subscriber.OnError,
                onComplete: () =>
                {
                    remaining--;
                    if (remaining == 0)
                    {
                        subscriber.OnCompleted();
                    }
                }));
        }

        return new Subscription(() =>
        {
            foreach (var subscription in subscriptions)
            {
                subscription.Dispose();
            }
        });
    });

    /// <summary>
    /// Wraps a .NET event (add/remove handler pair) as an <see cref="Observable{T}"/>. Mirrors rxjs's
    /// <c>fromEvent</c> and doubles as the C# analogue of Puppeteer's own <c>fromEmitterEvent</c> helper —
    /// .NET events are a fundamentally different shape from JS EventEmitters/DOM targets, so rather than
    /// port rxjs's many duck-typed overloads, this follows the established Rx.NET <c>FromEvent</c> idiom.
    /// </summary>
    public static Observable<TEventArgs> FromEvent<TDelegate, TEventArgs>(
        Action<TDelegate> addHandler,
        Action<TDelegate> removeHandler,
        Func<Action<TEventArgs>, TDelegate> conversion)
        => new Observable<TEventArgs>(subscriber =>
        {
            var handler = conversion(subscriber.OnNext);
            addHandler(handler);
            return new Subscription(() => removeHandler(handler));
        });

    /// <summary>The common case of <see cref="FromEvent{TDelegate, TEventArgs}"/> for standard <see cref="EventHandler{TEventArgs}"/>-shaped .NET events.</summary>
    public static Observable<TEventArgs> FromEvent<TEventArgs>(Action<EventHandler<TEventArgs>> addHandler, Action<EventHandler<TEventArgs>> removeHandler)
        => FromEvent<EventHandler<TEventArgs>, TEventArgs>(addHandler, removeHandler, onNext => (_, args) => onNext(args));
}

