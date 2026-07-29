namespace ReactiveExtensionsSharp.Operators;

/// <summary>Extension methods implementing the <c>switchAll</c> operator.</summary>
public static class SwitchAllOperator
{
    /// <summary>
    /// Converts a higher-order observable (an observable of observables) into a first-order observable that
    /// mirrors only the most recently produced inner observable, unsubscribing from any previous one as soon as
    /// a new one arrives. Equivalent to <c>source.SwitchMap(x =&gt; x)</c>.
    /// </summary>
    /// <typeparam name="T">The type of values emitted by the inner observables.</typeparam>
    /// <param name="source">The higher-order source sequence.</param>
    /// <returns>An observable that emits the values of only the most recently produced inner observable.</returns>
    public static Observable<T> SwitchAll<T>(this Observable<Observable<T>> source) => source.SwitchMap(inner => inner);
}
