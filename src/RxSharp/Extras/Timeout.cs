using RxSharp.Operators;

namespace RxSharp.Extras;

public static class TimeoutExtras
{
    /// <summary>An observable that never emits and errors after <paramref name="delay"/>, or never errors at all if <paramref name="delay"/> is zero. Mirrors Puppeteer's own <c>timeout()</c> helper.</summary>
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
