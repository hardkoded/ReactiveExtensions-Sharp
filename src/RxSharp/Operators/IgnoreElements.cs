namespace RxSharp.Operators;

public static class IgnoreElementsOperator
{
    public static Observable<T> IgnoreElements<T>(this Observable<T> source)
        => source.Operate<T, T>((src, subscriber) => src.Subscribe(onError: subscriber.OnError, onComplete: subscriber.OnCompleted));
}
