namespace RxSharp.Operators;

public static class MergeScanOperator
{
    public static Observable<TAcc> MergeScan<TSource, TAcc>(this Observable<TSource> source, Func<TAcc, TSource, int, Observable<TAcc>> accumulator, TAcc seed)
        => source.Operate<TSource, TAcc>((src, subscriber) =>
        {
            var state = seed;
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
                    Observable<TAcc> inner;
                    try
                    {
                        inner = accumulator(state, value, index++);
                    }
                    catch (Exception ex)
                    {
                        subscriber.OnError(ex);
                        return;
                    }

                    activeCount++;
                    var innerSubscription = inner.Subscribe(
                        onNext: result =>
                        {
                            state = result;
                            subscriber.OnNext(result);
                        },
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

    public static Observable<TAcc> MergeScan<TSource, TAcc>(this Observable<TSource> source, Func<TAcc, TSource, Observable<TAcc>> accumulator, TAcc seed)
        => source.MergeScan((acc, value, _) => accumulator(acc, value), seed);
}
