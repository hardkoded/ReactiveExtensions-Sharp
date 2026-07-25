namespace RxSharp.Operators;

public static class ThrowIfEmptyOperator
{
    public static Observable<T> ThrowIfEmpty<T>(this Observable<T> source, Func<Exception>? errorFactory = null)
    {
        var makeError = errorFactory ?? (() => new EmptyErrorException());
        return source.Operate<T, T>((src, subscriber) =>
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
                    if (hasValue)
                    {
                        subscriber.OnCompleted();
                    }
                    else
                    {
                        subscriber.OnError(makeError());
                    }
                });
        });
    }
}
