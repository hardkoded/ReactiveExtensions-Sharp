namespace ReactiveExtensionsSharp;

/// <summary>
/// An <see cref="Observable{T}"/>-shaped stream of values that all share a common <see cref="Key"/>. Mirrors
/// rxjs's <c>GroupedObservable</c>, the type emitted by <c>groupBy</c>. Declared as an interface (rather than a
/// subclass of <see cref="Observable{T}"/>, which is <see langword="sealed"/>) so it can still expose the same
/// two-overload <c>Subscribe</c> shape callers already get from <see cref="Observable{T}"/>.
/// </summary>
/// <typeparam name="TKey">The type of the shared key.</typeparam>
/// <typeparam name="T">The type of the grouped values.</typeparam>
public interface IGroupedObservable<TKey, T> : IObservable<T>
{
    /// <summary>Gets the key shared by every value in this group.</summary>
    TKey Key { get; }

    /// <summary>Subscribes to this group using plain delegates instead of a full <see cref="IObserver{T}"/>.</summary>
    /// <param name="onNext">Called for each value in the group.</param>
    /// <param name="onError">Called if the group errors.</param>
    /// <param name="onComplete">Called when the group closes.</param>
    /// <returns>A disposable that unsubscribes from the group.</returns>
    IDisposable Subscribe(Action<T>? onNext = null, Action<Exception>? onError = null, Action? onComplete = null);
}
