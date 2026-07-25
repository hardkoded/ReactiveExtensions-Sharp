using RxSharp.Subjects;

namespace RxSharp.Operators;

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
    public static Observable<T> ShareReplay<T>(this Observable<T> source, int bufferSize = int.MaxValue)
        => ShareCore.Multicast(source, () => new ReplaySubject<T>(bufferSize), resetOnComplete: false);
}
