using RxSharp.Subjects;

namespace RxSharp.Extras;

/// <summary>
/// A handle to an eagerly-attached, buffered .NET event source created by
/// <see cref="Extensions.FromEventBuffered{TDelegate, TEventArgs}"/>. Disposing detaches the underlying event handler.
/// </summary>
/// <typeparam name="TEventArgs">The type of the event's payload.</typeparam>
public sealed class BufferedEventSource<TEventArgs> : IDisposable
{
    private readonly ReplaySubject<TEventArgs> _subject;
    private readonly Action _detach;
    private bool _disposed;

    internal BufferedEventSource(ReplaySubject<TEventArgs> subject, Action detach)
    {
        _subject = subject;
        _detach = detach;
    }

    /// <summary>
    /// Exposes the buffered event payloads as an <see cref="Observable{T}"/>. Each subscriber first receives
    /// any payloads buffered since this source was created, then live payloads as they arrive - exactly like
    /// subscribing to a <see cref="ReplaySubject{T}"/> directly, because that is what this wraps.
    /// </summary>
    /// <returns>An observable over this source's buffered and live event payloads.</returns>
    public Observable<TEventArgs> AsObservable() => _subject.AsObservable();

    /// <summary>Detaches the underlying event handler and disposes the buffer. Safe to call more than once.</summary>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _detach();
        _subject.Dispose();
    }
}

/// <summary>Extension methods providing an eagerly-attached, buffered variant of <see cref="Observable.FromEvent{TEventArgs}"/>.</summary>
public static partial class Extensions
{
    /// <summary>
    /// Like <see cref="Observable.FromEvent{TDelegate, TEventArgs}"/>, but attaches the underlying event handler
    /// immediately - when this method is called - rather than lazily at subscribe time, buffering up to
    /// <paramref name="bufferSize"/> payloads that arrive before any subscriber attaches and replaying them to
    /// the first subscriber(s).
    /// </summary>
    /// <remarks>
    /// Ordinary <see cref="Observable.FromEvent{TDelegate, TEventArgs}"/> is cold: nothing is attached to the
    /// underlying event until <c>Subscribe</c> is called, matching every other RxSharp source. This is
    /// deliberately not that. It exists for the narrow case where a handler must be attached before
    /// synchronously checking some existing state that might already satisfy what the caller is waiting for
    /// (e.g. checking a collection for an already-matching item) without losing an event that fires in the real
    /// gap between attaching the raw handler and actually subscribing to the returned observable. That gap is
    /// provably real on .NET, where event delivery can run on another thread - unlike single-threaded JS, where
    /// rxjs's own <c>fromEmitterEvent</c> has no equivalent problem, since nothing can fire between two
    /// synchronous statements. Most code should use the ordinary, cold
    /// <see cref="Observable.FromEvent{TDelegate, TEventArgs}"/> instead; reach for this only when that gap is a
    /// real, provable race, not by default.
    /// </remarks>
    /// <typeparam name="TDelegate">The delegate type of the event handler.</typeparam>
    /// <typeparam name="TEventArgs">The type of the event's payload.</typeparam>
    /// <param name="addHandler">Called immediately, with the handler to add to the event.</param>
    /// <param name="removeHandler">Called on <see cref="BufferedEventSource{TEventArgs}.Dispose"/>, with the same handler, to remove it from the event.</param>
    /// <param name="conversion">Converts an <see cref="Action{TEventArgs}"/> callback into the event's actual delegate shape.</param>
    /// <param name="bufferSize">The maximum number of most-recent payloads kept for replay. Defaults to 1.</param>
    /// <returns>A handle exposing the buffered payloads as an observable, and detaching the handler on disposal.</returns>
    public static BufferedEventSource<TEventArgs> FromEventBuffered<TDelegate, TEventArgs>(
        Action<TDelegate> addHandler,
        Action<TDelegate> removeHandler,
        Func<Action<TEventArgs>, TDelegate> conversion,
        int bufferSize = 1)
    {
        var subject = new ReplaySubject<TEventArgs>(bufferSize);
        var handler = conversion(subject.OnNext);
        addHandler(handler);
        return new BufferedEventSource<TEventArgs>(subject, () => removeHandler(handler));
    }

    /// <summary>The common case of <see cref="FromEventBuffered{TDelegate, TEventArgs}"/> for standard <see cref="EventHandler{TEventArgs}"/>-shaped .NET events.</summary>
    /// <typeparam name="TEventArgs">The type of the event's payload.</typeparam>
    /// <param name="addHandler">Called immediately, with the handler to add to the event.</param>
    /// <param name="removeHandler">Called on <see cref="BufferedEventSource{TEventArgs}.Dispose"/>, with the same handler, to remove it from the event.</param>
    /// <param name="bufferSize">The maximum number of most-recent payloads kept for replay. Defaults to 1.</param>
    /// <returns>A handle exposing the buffered payloads as an observable, and detaching the handler on disposal.</returns>
    public static BufferedEventSource<TEventArgs> FromEventBuffered<TEventArgs>(
        Action<EventHandler<TEventArgs>> addHandler,
        Action<EventHandler<TEventArgs>> removeHandler,
        int bufferSize = 1)
        => FromEventBuffered<EventHandler<TEventArgs>, TEventArgs>(addHandler, removeHandler, onNext => (_, args) => onNext(args), bufferSize);
}
