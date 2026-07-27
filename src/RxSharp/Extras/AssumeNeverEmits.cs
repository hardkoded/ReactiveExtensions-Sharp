using RxSharp.Operators;

namespace RxSharp.Extras;

/// <summary>Extension methods widening an error-only <see cref="Observable{T}"/> to another element type.</summary>
public static class AssumeNeverEmitsExtras
{
    /// <summary>
    /// Widens an <see cref="Observable{Unit}"/> that only ever calls <see cref="IObserver{T}.OnError"/> (such as
    /// <see cref="CancellationExtras.FromCancellationToken"/> or <see cref="TimeoutExtras.Timeout"/>) so it
    /// type-checks anywhere an <see cref="Observable{T}"/> is expected - most commonly as one of the branches
    /// passed to <see cref="RaceOperator.RaceWith{T}"/> alongside a source that actually produces
    /// <typeparamref name="TResult"/> values. Mirrors how rxjs relies on TypeScript structurally accepting
    /// <c>Observable&lt;never&gt;</c> anywhere an <c>Observable&lt;T&gt;</c> is expected; C# has no bottom type,
    /// so this exists to fill that gap explicitly.
    /// </summary>
    /// <remarks>
    /// This is only safe for a <paramref name="source"/> that is guaranteed, by contract, to never call
    /// <see cref="IObserver{T}.OnNext"/>. If it ever does, the returned observable throws
    /// <see cref="InvalidOperationException"/> from that point on, converting a real value into a crash rather
    /// than propagating it - there is no compile-time check for this, unlike TypeScript's <c>never</c>. Do not
    /// apply this to a source that might legitimately emit.
    /// </remarks>
    /// <typeparam name="TResult">The element type to widen to.</typeparam>
    /// <param name="source">An observable that only ever errors, never emits.</param>
    /// <returns>An <see cref="Observable{TResult}"/> that mirrors <paramref name="source"/>'s errors and throws if it ever emits.</returns>
    public static Observable<TResult> AssumeNeverEmits<TResult>(this Observable<Unit> source)
        => source.Map<Unit, TResult>(_ => throw new InvalidOperationException(
            "AssumeNeverEmits: the source observable emitted a value, but was assumed to only ever error. " +
            "This is a contract violation in the caller, not in AssumeNeverEmits itself."));
}
