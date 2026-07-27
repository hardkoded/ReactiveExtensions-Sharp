using RxSharp.Operators;

namespace RxSharp.Extras;

/// <summary>Extension methods providing an async-predicate flavor of <c>Filter</c>.</summary>
public static partial class RxExtensions
{
    /// <summary>
    /// An operator supporting an async predicate, mirroring Puppeteer's own <c>filterAsync</c> helper (implemented,
    /// like theirs, via <c>mergeMap</c>). For each source value, <paramref name="predicate"/> is awaited and the
    /// value is re-emitted only if it resolves to <see langword="true"/>; values for which it resolves to
    /// <see langword="false"/> are dropped. Because the underlying <c>mergeMap</c> subscribes to every predicate
    /// task concurrently as source values arrive, results can be emitted out of the original source order if an
    /// earlier value's predicate task takes longer to complete than a later value's.
    /// </summary>
    /// <typeparam name="T">The type of the source (and, when kept, result) values.</typeparam>
    /// <param name="source">The source sequence to filter.</param>
    /// <param name="predicate">An async predicate evaluated for each source value.</param>
    /// <returns>An observable that emits only the source values whose predicate resolved to <see langword="true"/>.</returns>
    public static Observable<T> FilterAsync<T>(this Observable<T> source, Func<T, Task<bool>> predicate)
        => source.MergeMap(value => Observable.From(predicate(value)).Filter(matches => matches).Map(_ => value));
}
