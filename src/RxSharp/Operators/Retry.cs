namespace RxSharp.Operators;

public static class RetryOperator
{
    public static Observable<T> Retry<T>(this Observable<T> source, int count = int.MaxValue, TimeSpan? delay = null, IScheduler? scheduler = null)
    {
        if (count <= 0)
        {
            return source;
        }

        return source.Operate<T, T>((src, subscriber) =>
        {
            var soFar = 0;
            var innerSubscription = new SingleAssignmentDisposable();

            void SubscribeForRetry()
            {
                innerSubscription.Disposable = src.Subscribe(
                    onNext: subscriber.OnNext,
                    onError: err =>
                    {
                        if (soFar++ < count)
                        {
                            if (delay is { } d)
                            {
                                var notifierSubscription = new SingleAssignmentDisposable();
                                notifierSubscription.Disposable = Observable.Timer(d, scheduler).Subscribe(
                                    onNext: _ =>
                                    {
                                        notifierSubscription.Dispose();
                                        SubscribeForRetry();
                                    });
                            }
                            else
                            {
                                SubscribeForRetry();
                            }
                        }
                        else
                        {
                            subscriber.OnError(err);
                        }
                    },
                    onComplete: subscriber.OnCompleted);
            }

            SubscribeForRetry();
            return innerSubscription;
        });
    }
}
