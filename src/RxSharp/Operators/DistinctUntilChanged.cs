namespace RxSharp.Operators;

public static class DistinctUntilChangedOperator
{
    public static Observable<T> DistinctUntilChanged<T>(this Observable<T> source, IEqualityComparer<T>? comparer = null)
        => source.DistinctUntilChanged(comparer ?? EqualityComparer<T>.Default, x => x);

    public static Observable<T> DistinctUntilChanged<T, TKey>(this Observable<T> source, IEqualityComparer<TKey> comparer, Func<T, TKey> keySelector)
        => source.Operate<T, T>((src, subscriber) =>
        {
            var first = true;
            var previousKey = default(TKey);

            return src.Subscribe(
                onNext: value =>
                {
                    TKey currentKey;
                    try
                    {
                        currentKey = keySelector(value);
                    }
                    catch (Exception ex)
                    {
                        subscriber.OnError(ex);
                        return;
                    }

                    if (first || !comparer.Equals(previousKey!, currentKey))
                    {
                        first = false;
                        previousKey = currentKey;
                        subscriber.OnNext(value);
                    }
                },
                onError: subscriber.OnError,
                onComplete: subscriber.OnCompleted);
        });
}
