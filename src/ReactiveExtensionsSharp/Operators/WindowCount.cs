using ReactiveExtensionsSharp.Subjects;

namespace ReactiveExtensionsSharp.Operators;

/// <summary>The <c>windowCount</c> operator.</summary>
public static class WindowCountOperator
{
    /// <summary>
    /// Like <see cref="BufferCountOperator.BufferCount{T}"/>'s overlapping-window algorithm, but emits live
    /// <see cref="Observable{T}"/> windows (each backed by its own <see cref="Subject{T}"/>) instead of buffered
    /// arrays. Ported directly from rxjs's own <c>windowCount</c>: the first window opens eagerly at subscription
    /// time (so even an empty or immediately-erroring source still emits exactly one, empty, window), a new window
    /// opens every <paramref name="startWindowEvery"/> values (defaulting to <paramref name="windowSize"/>, i.e.
    /// non-overlapping windows), and the oldest still-open window completes once it has received
    /// <paramref name="windowSize"/> values — the two counters run independently, which is what produces
    /// overlapping windows when <paramref name="startWindowEvery"/> is less than <paramref name="windowSize"/>.
    /// </summary>
    /// <typeparam name="T">The element type of the source.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="windowSize">The number of values each window collects before closing.</param>
    /// <param name="startWindowEvery">How often (in source values) a new window opens; defaults to <paramref name="windowSize"/> for non-overlapping windows.</param>
    /// <returns>An observable of windows, each itself an observable of up to <paramref name="windowSize"/> values.</returns>
    public static Observable<Observable<T>> WindowCount<T>(this Observable<T> source, int windowSize, int? startWindowEvery = null)
    {
        var every = startWindowEvery is > 0 ? startWindowEvery.Value : windowSize;

        return source.Operate<T, Observable<T>>((src, subscriber) =>
        {
            var windows = new List<Subject<T>>();
            var count = 0;

            var firstWindow = new Subject<T>();
            windows.Add(firstWindow);
            subscriber.OnNext(firstWindow.AsObservable());

            return src.SubscribeChild(
                subscriber,
                onNext: value =>
                {
                    foreach (var window in windows)
                    {
                        window.OnNext(value);
                    }

                    var closeThreshold = count - windowSize + 1;
                    if (closeThreshold >= 0 && closeThreshold % every == 0 && windows.Count > 0)
                    {
                        var closing = windows[0];
                        windows.RemoveAt(0);
                        closing.OnCompleted();
                    }

                    count++;
                    if (count % every == 0)
                    {
                        var window = new Subject<T>();
                        windows.Add(window);
                        subscriber.OnNext(window.AsObservable());
                    }
                },
                onError: error =>
                {
                    foreach (var window in windows)
                    {
                        window.OnError(error);
                    }

                    windows.Clear();
                    subscriber.OnError(error);
                },
                onComplete: () =>
                {
                    foreach (var window in windows)
                    {
                        window.OnCompleted();
                    }

                    windows.Clear();
                    subscriber.OnCompleted();
                });
        });
    }
}
