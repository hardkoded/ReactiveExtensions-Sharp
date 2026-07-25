namespace RxSharp.Operators;

public static class ConcatMapOperator
{
    public static Observable<TResult> ConcatMap<TSource, TResult>(this Observable<TSource> source, Func<TSource, Observable<TResult>> project)
        => source.ConcatMap((value, _) => project(value));

    public static Observable<TResult> ConcatMap<TSource, TResult>(this Observable<TSource> source, Func<TSource, int, Observable<TResult>> project)
        => source.Operate<TSource, TResult>((src, subscriber) =>
        {
            var index = 0;
            var queue = new Queue<TSource>();
            var isSourceComplete = false;
            var isInnerActive = false;

            void SubscribeNext()
            {
                if (isInnerActive)
                {
                    return;
                }

                if (queue.Count == 0)
                {
                    if (isSourceComplete)
                    {
                        subscriber.OnCompleted();
                    }

                    return;
                }

                var value = queue.Dequeue();
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

                isInnerActive = true;
                var innerSubscription = new SingleAssignmentDisposable();
                innerSubscription.Disposable = inner.Subscribe(
                    onNext: subscriber.OnNext,
                    onError: subscriber.OnError,
                    onComplete: () =>
                    {
                        isInnerActive = false;
                        SubscribeNext();
                    });
                subscriber.Add(innerSubscription);
            }

            return src.Subscribe(
                onNext: value =>
                {
                    queue.Enqueue(value);
                    SubscribeNext();
                },
                onError: subscriber.OnError,
                onComplete: () =>
                {
                    isSourceComplete = true;
                    SubscribeNext();
                });
        });
}
