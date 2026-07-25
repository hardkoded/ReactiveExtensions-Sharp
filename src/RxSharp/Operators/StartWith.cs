namespace RxSharp.Operators;

public static class StartWithOperator
{
    public static Observable<T> StartWith<T>(this Observable<T> source, params T[] values)
        => source.Operate<T, T>((src, subscriber) =>
        {
            foreach (var value in values)
            {
                if (subscriber.IsDisposed)
                {
                    return null;
                }

                subscriber.OnNext(value);
            }

            return subscriber.IsDisposed
                ? null
                : src.Subscribe(onNext: subscriber.OnNext, onError: subscriber.OnError, onComplete: subscriber.OnCompleted);
        });
}
