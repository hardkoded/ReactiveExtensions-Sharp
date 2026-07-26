namespace RxSharp.Operators;

/// <summary>Extension methods implementing the <c>endWith</c> operator.</summary>
/// <remarks>
/// The mirror image of <see cref="StartWithOperator.StartWith{T}"/>: matches rxjs's own <c>endWith.ts</c>
/// exactly, which is literally <c>concat(source, of(...values))</c> — delegating to the existing, already-correct
/// <see cref="Observable.Concat{T}"/> and <see cref="Observable.Of{T}(T[])"/> rather than writing new subscribe
/// logic, so there is no new disposal-cascade risk to reason about here.
/// </remarks>
public static class EndWithOperator
{
    /// <summary>
    /// Returns an observable that mirrors <paramref name="source"/>, then, once it completes, synchronously
    /// emits <paramref name="values"/> in order before completing itself.
    /// </summary>
    /// <remarks>
    /// If <paramref name="source"/> errors, <paramref name="values"/> are never emitted — the error is
    /// forwarded immediately, matching <see cref="Observable.Concat{T}"/>'s own behavior.
    /// </remarks>
    /// <typeparam name="T">The type of values emitted by <paramref name="source"/> and in <paramref name="values"/>.</typeparam>
    /// <param name="source">The source sequence to emit before the trailing values.</param>
    /// <param name="values">The values to emit, in order, once <paramref name="source"/> completes.</param>
    /// <returns>An observable that emits <paramref name="source"/>'s values followed by <paramref name="values"/>.</returns>
    public static Observable<T> EndWith<T>(this Observable<T> source, params T[] values)
        => Observable.Concat(source, Observable.Of(values));
}
