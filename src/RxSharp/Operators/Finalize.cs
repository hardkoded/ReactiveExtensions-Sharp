namespace RxSharp.Operators;

/// <summary>Extension methods implementing the <c>finalize</c> operator.</summary>
public static class FinalizeOperator
{
    /// <summary>
    /// Mirrors <paramref name="source"/> exactly, but runs <paramref name="callback"/> exactly once when the
    /// resulting observable terminates — whether that's because <paramref name="source"/> completed, errored,
    /// or the subscriber unsubscribed early. This is rxjs's <c>try</c>/<c>finally</c> equivalent for observables.
    /// </summary>
    /// <remarks>
    /// Deliberately does not create any intermediate <see cref="Subscriber{T}"/>: <paramref name="source"/> is
    /// subscribed with the exact downstream <see cref="Subscriber{T}"/> this operator receives, matching rxjs's
    /// own <c>finalize.ts</c> (<c>source.subscribe(subscriber)</c>) almost verbatim. That is what makes this
    /// operator's disposal behavior automatically correct with zero extra bookkeeping: because there is no
    /// wrapper subscriber in between, a downstream disposal (e.g. an early-completing <c>Take</c> further down
    /// the chain) is visible to <paramref name="source"/> immediately, even for a fully-synchronous,
    /// self-checking source — there is nothing here that could introduce the disposal-cascade gap described in
    /// CLAUDE.md's Learnings, since this operator never creates the kind of intermediate subscriber that gap is
    /// about. <paramref name="callback"/> is registered as a teardown via <see cref="Subscription.Add(Action)"/>
    /// only after <paramref name="source"/>'s own subscribe call returns; if the subscriber is already stopped
    /// by then (completed, errored, or synchronously unsubscribed), <see cref="Subscription.Add(Action)"/> runs
    /// it immediately instead of deferring it, which is what lets <paramref name="callback"/> observe
    /// termination that already happened synchronously during subscribe.
    /// </remarks>
    /// <typeparam name="T">The type of values emitted by <paramref name="source"/>.</typeparam>
    /// <param name="source">The source sequence to finalize.</param>
    /// <param name="callback">The teardown logic to run exactly once, on completion, error, or unsubscription.</param>
    /// <returns>An observable that mirrors <paramref name="source"/>, invoking <paramref name="callback"/> on termination.</returns>
    public static Observable<T> Finalize<T>(this Observable<T> source, Action callback)
        => source.Operate<T, T>((src, subscriber) =>
        {
            try
            {
                src.Subscribe(subscriber);
            }
            finally
            {
                subscriber.Add(callback);
            }

            return null;
        });
}
