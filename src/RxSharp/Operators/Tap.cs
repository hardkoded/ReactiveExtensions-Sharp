namespace RxSharp.Operators;

public static class TapOperator
{
    public static Observable<T> Tap<T>(this Observable<T> source, Action<T>? onNext = null, Action<Exception>? onError = null, Action? onComplete = null)
        => source.Operate<T, T>((src, subscriber) => src.Subscribe(
            onNext: value =>
            {
                if (onNext is not null)
                {
                    try
                    {
                        onNext(value);
                    }
                    catch (Exception ex)
                    {
                        subscriber.OnError(ex);
                        return;
                    }
                }

                subscriber.OnNext(value);
            },
            onError: error =>
            {
                if (onError is not null)
                {
                    try
                    {
                        onError(error);
                    }
                    catch (Exception ex)
                    {
                        subscriber.OnError(ex);
                        return;
                    }
                }

                subscriber.OnError(error);
            },
            onComplete: () =>
            {
                if (onComplete is not null)
                {
                    try
                    {
                        onComplete();
                    }
                    catch (Exception ex)
                    {
                        subscriber.OnError(ex);
                        return;
                    }
                }

                subscriber.OnCompleted();
            }));
}
