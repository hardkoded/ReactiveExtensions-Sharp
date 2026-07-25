using RxSharp.Subjects;

namespace RxSharp.Operators;

/// <summary>Implements the <c>Share</c> operator. Mirrors rxjs's <c>share()</c> with its default config.</summary>
public static class ShareOperator
{
    /// <summary>
    /// Multicasts <paramref name="source"/> through an internal <see cref="Subject{T}"/>, subscribing to the
    /// source only once (on the first subscriber) and unsubscribing when the last subscriber leaves. A later
    /// subscription after the subscriber count drops back to zero reconnects to the source from scratch. See
    /// <see cref="ShareCore"/> for the shared refCount logic.
    /// </summary>
    /// <typeparam name="T">The element type of the source observable.</typeparam>
    /// <param name="source">The source observable to multicast.</param>
    /// <returns>A multicast observable that shares one subscription to <paramref name="source"/> among all its subscribers.</returns>
    public static Observable<T> Share<T>(this Observable<T> source)
        => ShareCore.Multicast(source, () => new Subject<T>(), resetOnComplete: true);
}
