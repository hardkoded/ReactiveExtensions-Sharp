namespace RxSharp.Operators;

public static class SwitchMapOperator
{
    public static Observable<TResult> SwitchMap<TSource, TResult>(this Observable<TSource> source, Func<TSource, Observable<TResult>> project)
        => source.SwitchMap((value, _) => project(value));

    public static Observable<TResult> SwitchMap<TSource, TResult>(this Observable<TSource> source, Func<TSource, int, Observable<TResult>> project)
        => source.Operate<TSource, TResult>((src, subscriber) =>
        {
            var index = 0;
            var isSourceComplete = false;
            var innerSubscription = new SingleAssignmentDisposable();
            var hasInner = false;
            subscriber.Add(innerSubscription);

            void CheckComplete()
            {
                if (isSourceComplete && !hasInner)
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

                    // SingleAssignmentDisposable only overwrites; switchMap needs the previous (still-live)
                    // inner subscription actually torn down before starting the new one.
                    innerSubscription.Disposable?.Dispose();
                    hasInner = true;
                    innerSubscription.Disposable = inner.Subscribe(
                        onNext: subscriber.OnNext,
                        onError: subscriber.OnError,
                        onComplete: () =>
                        {
                            hasInner = false;
                            CheckComplete();
                        });
                },
                onError: subscriber.OnError,
                onComplete: () =>
                {
                    isSourceComplete = true;
                    CheckComplete();
                });
        });
}
