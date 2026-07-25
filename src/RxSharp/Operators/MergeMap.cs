namespace RxSharp.Operators;

public static class MergeMapOperator
{
    public static Observable<TResult> MergeMap<TSource, TResult>(this Observable<TSource> source, Func<TSource, Observable<TResult>> project)
        => source.MergeMap((value, _) => project(value));

    public static Observable<TResult> MergeMap<TSource, TResult>(this Observable<TSource> source, Func<TSource, int, Observable<TResult>> project)
        => source.Operate<TSource, TResult>((src, subscriber) =>
        {
            var index = 0;
            var isSourceComplete = false;
            var activeCount = 0;

            void CheckComplete()
            {
                if (isSourceComplete && activeCount == 0)
                {
                    subscriber.OnCompleted();
                }
            }

            return src.Subscribe(
                onNext: value =>
                {
                    Observable<TResult> inner;
                    try
                    {
                        inner = project(value, index++);
                    }
                    catch (Exception ex)
                    {
                        subscriber.OnError(ex);
                        return;
                    }

                    activeCount++;
                    var innerSubscription = inner.Subscribe(
                        onNext: subscriber.OnNext,
                        onError: subscriber.OnError,
                        onComplete: () =>
                        {
                            activeCount--;
                            CheckComplete();
                        });
                    subscriber.Add(innerSubscription);
                },
                onError: subscriber.OnError,
                onComplete: () =>
                {
                    isSourceComplete = true;
                    CheckComplete();
                });
        });
}
