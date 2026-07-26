namespace RxSharp.Operators;

/// <summary>Extension methods implementing the <c>timestamp</c> operator.</summary>
public static class TimestampOperator
{
    /// <summary>
    /// Tags each value emitted by <paramref name="source"/> with the time it was received, using
    /// <paramref name="scheduler"/>'s notion of "now". Errors and completion are passed through unchanged.
    /// </summary>
    /// <typeparam name="T">The type of values emitted by <paramref name="source"/>.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="scheduler">The scheduler whose <see cref="IScheduler.Now"/> is used to time each emission; defaults to <see cref="TaskPoolScheduler.Instance"/> when <see langword="null"/>.</param>
    /// <returns>An observable of <see cref="RxSharp.Timestamp{T}"/> values pairing each source value with the time it was received.</returns>
    public static Observable<Timestamp<T>> Timestamp<T>(this Observable<T> source, IScheduler? scheduler = null)
    {
        var activeScheduler = scheduler ?? TaskPoolScheduler.Instance;
        return source.Map(value => new Timestamp<T>(value, activeScheduler.Now));
    }
}
