using RxSharp.Subjects;

namespace RxSharp.Operators;

public static class ShareOperator
{
    /// <summary>
    /// Multicasts <paramref name="source"/> through an internal <see cref="Subject{T}"/>, subscribing to the
    /// source only once (on the first subscriber) and unsubscribing when the last subscriber leaves. A later
    /// subscription after the subscriber count drops back to zero reconnects to the source from scratch. Mirrors
    /// rxjs's <c>share()</c> with its default config. See <see cref="ShareCore"/> for the shared refCount logic.
    /// </summary>
    public static Observable<T> Share<T>(this Observable<T> source)
        => ShareCore.Multicast(source, () => new Subject<T>(), resetOnComplete: true);
}
