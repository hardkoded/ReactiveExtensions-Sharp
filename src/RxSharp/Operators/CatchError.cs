namespace RxSharp.Operators;

public static class CatchErrorOperator
{
    public static Observable<T> CatchError<T>(this Observable<T> source, Func<Exception, Observable<T>> selector)
        => source.Operate<T, T>((src, subscriber) =>
        {
            var subscription = new SingleAssignmentDisposable();
            subscription.Disposable = src.Subscribe(
                onNext: subscriber.OnNext,
                onError: error =>
                {
                    Observable<T> replacement;
                    try
                    {
                        replacement = selector(error);
                    }
                    catch (Exception ex)
                    {
                        subscriber.OnError(ex);
                        return;
                    }

                    subscription.Disposable = replacement.Subscribe(subscriber);
                },
                onComplete: subscriber.OnCompleted);

            return subscription;
        });
}
