namespace ReactiveExtensionsSharp;

/// <summary>Bridges an <see cref="Observable{T}"/> to a <see cref="Task{T}"/>. Mirrors rxjs's <c>firstValueFrom</c>/<c>lastValueFrom</c>.</summary>
public static class AsyncBridge
{
    /// <summary>Subscribes to <paramref name="source"/> and returns a task that completes with its first emitted value, then unsubscribes. Faults with <see cref="EmptyErrorException"/> if the source completes without emitting.</summary>
    /// <typeparam name="T">The element type of the source.</typeparam>
    /// <param name="source">The observable to take the first value from.</param>
    /// <returns>A task that resolves to the source's first value.</returns>
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

    /// <summary>Subscribes to <paramref name="source"/> and returns a task that completes with its first emitted value, then unsubscribes. Resolves to <paramref name="defaultValue"/> instead of faulting if the source completes without emitting.</summary>
    /// <typeparam name="T">The element type of the source.</typeparam>
    /// <param name="source">The observable to take the first value from.</param>
    /// <param name="defaultValue">The value to resolve with if the source completes without emitting.</param>
    /// <returns>A task that resolves to the source's first value, or <paramref name="defaultValue"/>.</returns>
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

    /// <summary>Subscribes to <paramref name="source"/> and returns a task that completes with its last emitted value once the source completes. Faults with <see cref="EmptyErrorException"/> if the source completes without ever emitting.</summary>
    /// <typeparam name="T">The element type of the source.</typeparam>
    /// <param name="source">The observable to take the last value from.</param>
    /// <returns>A task that resolves to the source's last value once it completes.</returns>
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

    /// <summary>Subscribes to <paramref name="source"/> and returns a task that completes with its last emitted value once the source completes. Resolves to <paramref name="defaultValue"/> instead of faulting if the source completes without ever emitting.</summary>
    /// <typeparam name="T">The element type of the source.</typeparam>
    /// <param name="source">The observable to take the last value from.</param>
    /// <param name="defaultValue">The value to resolve with if the source completes without emitting.</param>
    /// <returns>A task that resolves to the source's last value, or <paramref name="defaultValue"/>.</returns>
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
