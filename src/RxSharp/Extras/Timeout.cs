using RxSharp.Operators;

namespace RxSharp.Extras;

/// <summary>Extension methods providing a standalone, <see cref="TimeoutException"/>-throwing timer observable.</summary>
public static class TimeoutExtras
{
    /// <summary>
    /// An observable that never emits and errors after <paramref name="delay"/> elapses, or never errors at all if
    /// <paramref name="delay"/> is zero or negative (in which case the returned observable is <see cref="Observable.Never{T}"/>
    /// and completes/errors are both suppressed forever). Mirrors Puppeteer's own <c>timeout()</c> helper. Internally
    /// built from <see cref="Observable.Timer"/>, so the <paramref name="delay"/> is scheduled on
    /// <paramref name="scheduler"/> like any other scheduled operator.
    /// </summary>
    /// <param name="delay">The duration to wait before erroring. A zero or negative value disables the timeout entirely.</param>
    /// <param name="causeFactory">
    /// Produces the exception thrown once <paramref name="delay"/> elapses. Defaults to a new <see cref="TimeoutException"/>.
    /// </param>
    /// <param name="scheduler">The scheduler used to run the timer. Defaults to the scheduler <see cref="Observable.Timer"/> itself defaults to.</param>
    /// <returns>An observable that never calls <see cref="IObserver{T}.OnNext"/> and, unless <paramref name="delay"/> is non-positive, errors once it elapses.</returns>
    public static Observable<Unit> Timeout(TimeSpan delay, Func<Exception>? causeFactory = null, IScheduler? scheduler = null)
    {
        if (delay <= TimeSpan.Zero)
        {
            return Observable.Never<Unit>();
        }

        var makeCause = causeFactory ?? DefaultCause;
        return Observable.Timer(delay, scheduler).Map<long, Unit>(_ => throw makeCause());
    }

    private static Exception DefaultCause() => new TimeoutException();
}
