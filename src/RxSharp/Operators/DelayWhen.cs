namespace RxSharp.Operators;

public static class DelayWhenOperator
{
    public static Observable<T> DelayWhen<T, TDuration>(this Observable<T> source, Func<T, int, Observable<TDuration>> delayDurationSelector)
        => source.MergeMap((value, index) => delayDurationSelector(value, index).Take(1).Map(_ => value));

    public static Observable<T> DelayWhen<T, TDuration>(this Observable<T> source, Func<T, Observable<TDuration>> delayDurationSelector)
        => source.DelayWhen((value, _) => delayDurationSelector(value));
}
