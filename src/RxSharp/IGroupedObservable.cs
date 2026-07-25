namespace RxSharp;

/// <summary>
/// An <see cref="Observable{T}"/>-shaped stream of values that all share a common <see cref="Key"/>. Mirrors
/// rxjs's <c>GroupedObservable</c>, the type emitted by <c>groupBy</c>. Declared as an interface (rather than a
/// subclass of <see cref="Observable{T}"/>, which is <see langword="sealed"/>) so it can still expose the same
/// two-overload <c>Subscribe</c> shape callers already get from <see cref="Observable{T}"/>.
/// </summary>
public interface IGroupedObservable<TKey, T> : IObservable<T>
{
    TKey Key { get; }

    IDisposable Subscribe(Action<T>? onNext = null, Action<Exception>? onError = null, Action? onComplete = null);
}
