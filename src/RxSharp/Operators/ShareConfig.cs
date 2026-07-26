using RxSharp.Subjects;

namespace RxSharp.Operators;

/// <summary>
/// Configuration object for the <see cref="ShareOperator.Share{T}(Observable{T}, ShareConfig{T})"/> overload,
/// mirroring rxjs's <c>ShareConfig</c>. All properties default to rxjs's own defaults
/// (<c>resetOnError: true, resetOnComplete: true, resetOnRefCountZero: true</c>, plain <see cref="Subject{T}"/>
/// connector), so <c>new ShareConfig&lt;T&gt;()</c> behaves identically to the parameterless <see cref="ShareOperator.Share{T}(Observable{T})"/>.
/// </summary>
/// <remarks>
/// Deliberate deviation from rxjs: <see cref="ResetOnError"/>, <see cref="ResetOnComplete"/>, and
/// <see cref="ResetOnRefCountZero"/> are plain <see cref="bool"/> only here. Real rxjs also accepts a notifier
/// factory (<c>(error?) => ObservableInput&lt;any&gt;</c>) for each, letting the reset be delayed/conditional on
/// some other observable emitting. That variant is not implemented -- it adds substantial complexity (async/deferred
/// reset timing, unhandled-error-on-notifier-error semantics) for a corner of the API this port's target use case
/// (Puppeteer-style retry/timeout combinators) never needs. <see cref="RxSharp.ShareCore"/> already carried this
/// same "boolean case only" note for the completion knob before this config object existed; it now applies
/// uniformly to all three reset knobs.
/// </remarks>
/// <typeparam name="T">The element type of the observable being shared.</typeparam>
public sealed class ShareConfig<T>
{
    /// <summary>
    /// Factory used to create the <see cref="Subject{T}"/>-like connector that multicasts the source to
    /// subscribers. Defaults to a plain <see cref="Subject{T}"/> (matches rxjs's default). Pass e.g.
    /// <c>() =&gt; new BehaviorSubject&lt;T&gt;(initialValue)</c> to get "replay the latest value" semantics
    /// with configurable reset behavior -- <see cref="ShareReplayOperator.ShareReplay{T}"/> already covers the
    /// dedicated <see cref="ReplaySubject{T}"/>-backed caching case, so this knob's main remaining value is
    /// exactly this: other connector types (<see cref="BehaviorSubject{T}"/>, <see cref="AsyncSubject{T}"/>, or a
    /// caller's own <see cref="Subject{T}"/> subclass) that don't have their own dedicated Share* operator.
    /// </summary>
    public Func<Subject<T>>? Connector { get; set; }

    /// <summary>
    /// If <see langword="true"/> (the default), an error from the source resets the internal state back to
    /// "cold" -- a later subscriber gets a fresh connector and a fresh subscription to the source. If
    /// <see langword="false"/>, the error is pushed into the connector and the connector is kept: subsequent
    /// subscribers attach to that same, now-errored connector and immediately receive the same error, and the
    /// source is never resubscribed.
    /// </summary>
    public bool ResetOnError { get; set; } = true;

    /// <summary>
    /// If <see langword="true"/> (the default), the source completing resets the internal state back to "cold" --
    /// a later subscriber gets a fresh connector and a fresh subscription to the source. If <see langword="false"/>,
    /// completion is pushed into the connector and the connector is kept: subsequent subscribers attach to that
    /// same, now-completed connector and immediately receive completion, and the source is never resubscribed.
    /// </summary>
    public bool ResetOnComplete { get; set; } = true;

    /// <summary>
    /// If <see langword="true"/> (the default), the subscriber count dropping back to zero (purely from
    /// unsubscription, not from the source erroring/completing) resets the internal state: the source subscription
    /// is torn down, and a later subscriber reconnects from scratch. If <see langword="false"/>, the source
    /// subscription is kept alive with nobody listening, so a later subscriber reattaches to the same connector
    /// and the same still-live source subscription instead of resubscribing.
    /// </summary>
    public bool ResetOnRefCountZero { get; set; } = true;
}
