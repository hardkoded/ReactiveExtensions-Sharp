namespace RxSharp.Operators;

public static class FilterOperator
{
    public static Observable<T> Filter<T>(this Observable<T> source, Func<T, bool> predicate)
        => source.Filter((value, _) => predicate(value));

    public static Observable<T> Filter<T>(this Observable<T> source, Func<T, int, bool> predicate)
        => source.Operate<T, T>((src, subscriber) =>
        {
            var index = 0;
            return src.Subscribe(
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
                    }
                },
                onError: subscriber.OnError,
                onComplete: subscriber.OnCompleted);
        });
}
