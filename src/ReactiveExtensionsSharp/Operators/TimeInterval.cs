namespace ReactiveExtensionsSharp.Operators;

/// <summary>Extension methods implementing the <c>timeInterval</c> operator.</summary>
public static class TimeIntervalOperator
{
    /// <summary>
    /// Tags each value emitted by <paramref name="source"/> with the time elapsed since the previous emission
    /// (or since subscription, for the first emission), using <paramref name="scheduler"/>'s notion of "now".
    /// Errors and completion are passed through unchanged.
    /// </summary>
    /// <typeparam name="T">The type of values emitted by <paramref name="source"/>.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="scheduler">The scheduler whose <see cref="IScheduler.Now"/> is used to time each emission; defaults to <see cref="TaskPoolScheduler.Instance"/> when <see langword="null"/>.</param>
    /// <returns>An observable of <see cref="ReactiveExtensionsSharp.TimeInterval{T}"/> values pairing each source value with the time elapsed since the previous one.</returns>
    public static Observable<TimeInterval<T>> TimeInterval<T>(this Observable<T> source, IScheduler? scheduler = null)
        => source.Operate<T, TimeInterval<T>>((src, subscriber) =>
        {
            var activeScheduler = scheduler ?? TaskPoolScheduler.Instance;
            var last = activeScheduler.Now;

            return src.SubscribeChild(
                subscriber,
                onNext: value =>
                {
                    var now = activeScheduler.Now;
                    var interval = now - last;
                    last = now;
                    subscriber.OnNext(new TimeInterval<T>(value, interval));
                },
                onError: subscriber.OnError,
                onComplete: subscriber.OnCompleted);
        });
}
