namespace RxSharp.Operators;

public static class TakeUntilOperator
{
    public static Observable<T> TakeUntil<T, TNotifier>(this Observable<T> source, Observable<TNotifier> notifier)
        => source.Operate<T, T>((src, subscriber) =>
        {
            var notifierSubscription = notifier.Subscribe(onNext: _ => subscriber.OnCompleted());
            subscriber.Add(notifierSubscription);

            return subscriber.IsDisposed
                ? null
                : src.Subscribe(onNext: subscriber.OnNext, onError: subscriber.OnError, onComplete: subscriber.OnCompleted);
        });
}
