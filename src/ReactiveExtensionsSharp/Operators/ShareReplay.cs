using ReactiveExtensionsSharp.Subjects;

namespace ReactiveExtensionsSharp.Operators;

/// <summary>Implements the <c>ShareReplay</c> operator. Mirrors rxjs's <c>shareReplay()</c>.</summary>
public static class ShareReplayOperator
{
    /// <summary>
    /// Multicasts <paramref name="source"/> through an internal <see cref="ReplaySubject{T}"/>, so late
    /// subscribers immediately receive the last <paramref name="bufferSize"/> values (unbounded by default,
    /// matching <see cref="ReplaySubject{T}"/>'s own default). Subscribes to the source only once (on the first
    /// subscriber). Once the source completes, its buffered values keep replaying to every later subscriber
    /// forever (the source is never resubscribed) -- matches rxjs's <c>resetOnComplete: false</c> default, and
    /// is the whole reason to reach for <c>shareReplay</c> over <c>share</c> (e.g. caching a completed request).
    /// If the source errors instead, the connector resets and a later subscriber gets a fresh
    /// <see cref="ReplaySubject{T}"/> and a fresh subscription to the source (matches rxjs's
    /// <c>resetOnError: true</c> default).
    /// <para>
    /// Deliberate deviation from rxjs: real rxjs's <c>shareReplay()</c> also defaults <c>refCount: false</c> --
    /// even while the source is still live, it never unsubscribes when every subscriber leaves, only when the
    /// source itself errors/completes. This port always disconnects once the subscriber count drops to zero
    /// (the <c>refCount: true</c> mode) rather than leaking a live subscription forever with nobody listening;
    /// there's no knob to opt into the leaky default.
    /// </para>
    /// </summary>
    /// <typeparam name="T">The element type of the source observable.</typeparam>
    /// <param name="source">The source observable to multicast.</param>
    /// <param name="bufferSize">The maximum number of most-recent values to replay to each new subscriber. Unbounded by default.</param>
    /// <returns>A multicast, replaying observable that shares one subscription to <paramref name="source"/> among all its subscribers.</returns>
    public static Observable<T> ShareReplay<T>(this Observable<T> source, int bufferSize = int.MaxValue)
        => ShareCore.Multicast(source, () => new ReplaySubject<T>(bufferSize), resetOnError: true, resetOnComplete: false, resetOnRefCountZero: true);
}
