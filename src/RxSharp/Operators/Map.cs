namespace RxSharp.Operators;

public static class MapOperator
{
    public static Observable<TResult> Map<TSource, TResult>(this Observable<TSource> source, Func<TSource, TResult> project)
        => source.Map((value, _) => project(value));

    public static Observable<TResult> Map<TSource, TResult>(this Observable<TSource> source, Func<TSource, int, TResult> project)
        => source.Operate<TSource, TResult>((src, subscriber) =>
        {
            var index = 0;
            return src.Subscribe(
                onNext: value =>
                {
                    TResult result;
                    try
                    {
                        result = project(value, index++);
                    }
                    catch (Exception ex)
                    {
                        subscriber.OnError(ex);
                        return;
                    }

                    subscriber.OnNext(result);
                },
                onError: subscriber.OnError,
                onComplete: subscriber.OnCompleted);
        });
}
