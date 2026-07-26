using RxSharp.Subjects;

namespace RxSharp.Operators;

/// <summary>Implements the <c>Share</c> operator. Mirrors rxjs's <c>share()</c> with its default config.</summary>
public static class ShareOperator
{
    /// <summary>
    /// Multicasts <paramref name="source"/> through an internal <see cref="Subject{T}"/>, subscribing to the
    /// source only once (on the first subscriber) and unsubscribing when the last subscriber leaves. A later
    /// subscription after the subscriber count drops back to zero reconnects to the source from scratch. See
    /// <see cref="ShareCore"/> for the shared refCount logic. Equivalent to
    /// <c>source.Share(new ShareConfig&lt;T&gt;())</c> -- rxjs's own defaults
    /// (<c>resetOnError: true, resetOnComplete: true, resetOnRefCountZero: true</c>, plain <see cref="Subject{T}"/>).
    /// </summary>
    /// <typeparam name="T">The element type of the source observable.</typeparam>
    /// <param name="source">The source observable to multicast.</param>
    /// <returns>A multicast observable that shares one subscription to <paramref name="source"/> among all its subscribers.</returns>
    public static Observable<T> Share<T>(this Observable<T> source)
        => ShareCore.Multicast(source, () => new Subject<T>(), resetOnError: true, resetOnComplete: true, resetOnRefCountZero: true);

    /// <summary>
    /// Multicasts <paramref name="source"/> the same way as <see cref="Share{T}(Observable{T})"/>, but with each
    /// reset trigger and the connector factory configurable via <paramref name="config"/>. See
    /// <see cref="ShareConfig{T}"/> for what each option controls.
    /// </summary>
    /// <typeparam name="T">The element type of the source observable.</typeparam>
    /// <param name="source">The source observable to multicast.</param>
    /// <param name="config">The reset/connector configuration to use.</param>
    /// <returns>A multicast observable that shares one subscription to <paramref name="source"/> among all its subscribers.</returns>
    public static Observable<T> Share<T>(this Observable<T> source, ShareConfig<T> config)
        => ShareCore.Multicast(
            source,
            config.Connector ?? (() => new Subject<T>()),
            config.ResetOnError,
            config.ResetOnComplete,
            config.ResetOnRefCountZero);
}
