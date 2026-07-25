namespace RxSharp.Operators;

public static class TakeOperator
{
    public static Observable<T> Take<T>(this Observable<T> source, int count)
    {
        if (count <= 0)
        {
            return Observable.Empty<T>();
        }

        return source.Operate<T, T>((src, subscriber) =>
        {
            var seen = 0;
            var sourceSubscription = new SingleAssignmentDisposable();
            sourceSubscription.Disposable = src.Subscribe(
                onNext: value =>
                {
                    if (++seen <= count)
                    {
                        subscriber.OnNext(value);
                        if (seen >= count)
                        {
                            subscriber.OnCompleted();
                            sourceSubscription.Dispose();
                        }
                    }
                },
                onError: subscriber.OnError,
                onComplete: subscriber.OnCompleted);

            return sourceSubscription;
        });
    }
}
