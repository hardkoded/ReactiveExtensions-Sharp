using RxSharp.Operators;

namespace RxSharp.Extras;

public static class FilterAsyncExtras
{
    /// <summary>An operator supporting an async predicate, mirroring Puppeteer's own <c>filterAsync</c> helper (implemented, like theirs, via <c>mergeMap</c>).</summary>
    public static Observable<T> FilterAsync<T>(this Observable<T> source, Func<T, Task<bool>> predicate)
        => source.MergeMap(value => Observable.From(predicate(value)).Filter(matches => matches).Map(_ => value));
}
