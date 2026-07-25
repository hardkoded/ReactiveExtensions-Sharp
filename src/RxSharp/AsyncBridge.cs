namespace RxSharp;

/// <summary>Bridges an <see cref="Observable{T}"/> to a <see cref="Task{T}"/>. Mirrors rxjs's <c>firstValueFrom</c>/<c>lastValueFrom</c>.</summary>
public static class AsyncBridge
{
    public static Task<T> FirstValueFrom<T>(this Observable<T> source)
    {
        var tcs = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscription = new SingleAssignmentDisposable();
        subscription.Disposable = source.Subscribe(
            onNext: value =>
            {
                tcs.TrySetResult(value);
                subscription.Dispose();
            },
            onError: err => tcs.TrySetException(err),
            onComplete: () => tcs.TrySetException(new EmptyErrorException()));

        return tcs.Task;
    }

    public static Task<T> FirstValueFrom<T>(this Observable<T> source, T defaultValue)
    {
        var tcs = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscription = new SingleAssignmentDisposable();
        subscription.Disposable = source.Subscribe(
            onNext: value =>
            {
                tcs.TrySetResult(value);
                subscription.Dispose();
            },
            onError: err => tcs.TrySetException(err),
            onComplete: () => tcs.TrySetResult(defaultValue));

        return tcs.Task;
    }

    public static Task<T> LastValueFrom<T>(this Observable<T> source)
    {
        var tcs = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);
        var hasValue = false;
        T last = default!;
        source.Subscribe(
            onNext: value =>
            {
                last = value;
                hasValue = true;
            },
            onError: err => tcs.TrySetException(err),
            onComplete: () =>
            {
                if (hasValue)
                {
                    tcs.TrySetResult(last);
                }
                else
                {
                    tcs.TrySetException(new EmptyErrorException());
                }
            });

        return tcs.Task;
    }

    public static Task<T> LastValueFrom<T>(this Observable<T> source, T defaultValue)
    {
        var tcs = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);
        var hasValue = false;
        T last = default!;
        source.Subscribe(
            onNext: value =>
            {
                last = value;
                hasValue = true;
            },
            onError: err => tcs.TrySetException(err),
            onComplete: () => tcs.TrySetResult(hasValue ? last : defaultValue));

        return tcs.Task;
    }
}
