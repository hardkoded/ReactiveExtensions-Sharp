namespace RxSharp.Operators;

public static class DefaultIfEmptyOperator
{
    public static Observable<T> DefaultIfEmpty<T>(this Observable<T> source, T defaultValue)
        => source.Operate<T, T>((src, subscriber) =>
        {
            var hasValue = false;
            return src.Subscribe(
                onNext: value =>
                {
                    hasValue = true;
                    subscriber.OnNext(value);
                },
                onError: subscriber.OnError,
                onComplete: () =>
                {
                    if (!hasValue)
                    {
                        subscriber.OnNext(defaultValue);
                    }

                    subscriber.OnCompleted();
                });
        });
}
