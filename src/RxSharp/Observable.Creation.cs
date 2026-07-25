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
}

