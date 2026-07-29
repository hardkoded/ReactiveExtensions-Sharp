namespace ReactiveExtensionsSharp.Operators;

/// <summary>Extension methods implementing the <c>exhaustAll</c> operator.</summary>
public static class ExhaustAllOperator
{
    /// <summary>
    /// Converts a higher-order observable (an observable of observables) into a first-order observable by
    /// subscribing to an inner observable and ignoring every other inner observable produced while it is still
    /// active. Equivalent to <c>source.ExhaustMap(x =&gt; x)</c>.
    /// </summary>
    /// <typeparam name="T">The type of values emitted by the inner observables.</typeparam>
    /// <param name="source">The higher-order source sequence.</param>
    /// <returns>An observable that emits the values of each accepted inner observable, ignoring inner observables produced while one is still active.</returns>
    public static Observable<T> ExhaustAll<T>(this Observable<Observable<T>> source) => source.ExhaustMap(inner => inner);
}
