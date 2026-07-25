namespace RxSharp.Operators;

public static class FirstOperator
{
    public static Observable<T> First<T>(this Observable<T> source)
        => source.FirstCore((_, _) => true, hasDefault: false, default!);

    public static Observable<T> First<T>(this Observable<T> source, T defaultValue)
        => source.FirstCore((_, _) => true, hasDefault: true, defaultValue);

    public static Observable<T> First<T>(this Observable<T> source, Func<T, bool> predicate)
        => source.FirstCore((value, _) => predicate(value), hasDefault: false, default!);

    public static Observable<T> First<T>(this Observable<T> source, Func<T, bool> predicate, T defaultValue)
        => source.FirstCore((value, _) => predicate(value), hasDefault: true, defaultValue);

    public static Observable<T> First<T>(this Observable<T> source, Func<T, int, bool> predicate)
        => source.FirstCore(predicate, hasDefault: false, default!);

    public static Observable<T> First<T>(this Observable<T> source, Func<T, int, bool> predicate, T defaultValue)
        => source.FirstCore(predicate, hasDefault: true, defaultValue);

    private static Observable<T> FirstCore<T>(this Observable<T> source, Func<T, int, bool> predicate, bool hasDefault, T defaultValue)
        => source.Operate<T, T>((src, subscriber) =>
        {
            var index = 0;
            var sourceSubscription = new SingleAssignmentDisposable();
            sourceSubscription.Disposable = src.Subscribe(
                onNext: value =>
                {
                    bool matches;
                    try
                    {
                        matches = predicate(value, index++);
                    }
                    catch (Exception ex)
                    {
                        subscriber.OnError(ex);
                        return;
                    }

                    if (matches)
                    {
                        subscriber.OnNext(value);
                        subscriber.OnCompleted();
                        sourceSubscription.Dispose();
                    }
                },
                onError: subscriber.OnError,
                onComplete: () =>
                {
                    if (hasDefault)
                    {
                        subscriber.OnNext(defaultValue);
                        subscriber.OnCompleted();
                    }
                    else
                    {
                        subscriber.OnError(new EmptyErrorException());
                    }
                });

            return sourceSubscription;
        });
}
