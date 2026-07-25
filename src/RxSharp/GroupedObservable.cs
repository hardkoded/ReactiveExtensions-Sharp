using RxSharp.Subjects;

namespace RxSharp;

/// <summary>
/// Concrete <see cref="IGroupedObservable{TKey, T}"/> used by <c>GroupBy</c>. Backed by a <see cref="Subject{T}"/>
/// so it multicasts to every subscriber (matching rxjs's own <c>GroupedObservable</c>, which is itself a thin
/// wrapper around a per-group <c>Subject</c>).
/// </summary>
/// <typeparam name="TKey">The type of the shared key.</typeparam>
/// <typeparam name="T">The type of the grouped values.</typeparam>
public sealed class GroupedObservable<TKey, T> : IGroupedObservable<TKey, T>
{
    private readonly Subject<T> _subject;

    internal GroupedObservable(TKey key, Subject<T> subject)
    {
        Key = key;
        _subject = subject;
    }

    /// <inheritdoc/>
    public TKey Key { get; }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer) => _subject.Subscribe(observer);

    /// <inheritdoc/>
    public IDisposable Subscribe(Action<T>? onNext = null, Action<Exception>? onError = null, Action? onComplete = null)
        => _subject.Subscribe(onNext, onError, onComplete);
}
