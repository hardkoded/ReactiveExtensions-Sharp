namespace RxSharp;

public delegate Observable<TResult> OperatorFunction<TSource, TResult>(Observable<TSource> source);

public delegate Observable<T> MonoTypeOperatorFunction<T>(Observable<T> source);

/// <summary>The building block every operator is written against. Mirrors rxjs's <c>operate()</c> helper.</summary>
public static class OperatorHelper
{
    public static Observable<TResult> Operate<TSource, TResult>(
        this Observable<TSource> source,
        Func<Observable<TSource>, Subscriber<TResult>, IDisposable?> init)
        => new Observable<TResult>(subscriber => init(source, subscriber));

    public static Observable<TResult> Pipe<TSource, TResult>(this Observable<TSource> source, OperatorFunction<TSource, TResult> op)
        => op(source);
}
