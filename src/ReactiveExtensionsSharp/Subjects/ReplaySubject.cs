namespace ReactiveExtensionsSharp.Subjects;

/// <summary>
/// A <see cref="Subject{T}"/> that buffers emitted values and replays them to new subscribers before they start
/// receiving live values. Mirrors rxjs's <c>ReplaySubject</c>.
/// </summary>
/// <typeparam name="T">The type of values pushed through the subject.</typeparam>
public sealed class ReplaySubject<T> : Subject<T>
{
    private readonly int _bufferSize;
    private readonly TimeSpan? _windowTime;
    private readonly Func<DateTimeOffset> _clock;
    private readonly Queue<(DateTimeOffset Timestamp, T Value)> _buffer = new Queue<(DateTimeOffset Timestamp, T Value)>();
    private readonly object _bufferGate = new object();

    /// <summary>
    /// Initializes a new instance of the <see cref="ReplaySubject{T}"/> class.
    /// </summary>
    /// <param name="bufferSize">
    /// The maximum number of most-recent values kept for replay. Defaults to <see cref="int.MaxValue"/>, i.e.
    /// effectively unbounded — every value ever pushed is replayed to new subscribers unless trimmed by
    /// <paramref name="windowTime"/>.
    /// </param>
    /// <param name="windowTime">
    /// The maximum age of a buffered value, relative to <paramref name="clock"/>. Values older than this are
    /// dropped from the buffer. Defaults to <see langword="null"/>, meaning no age-based trimming — only
    /// <paramref name="bufferSize"/> bounds the buffer.
    /// </param>
    /// <param name="clock">Supplies the current time used to timestamp buffered values and evaluate <paramref name="windowTime"/>. Defaults to <see cref="DateTimeOffset.UtcNow"/>.</param>
    public ReplaySubject(int bufferSize = int.MaxValue, TimeSpan? windowTime = null, Func<DateTimeOffset>? clock = null)
    {
        _bufferSize = bufferSize;
        _windowTime = windowTime;
        _clock = clock ?? (() => DateTimeOffset.UtcNow);
    }

    /// <summary>
    /// Buffers <paramref name="value"/> (subject to trimming by buffer size/window time) and then pushes it to
    /// every observer currently subscribed. The stopped/disposed check happens before buffering, not just before
    /// forwarding — a value nexted after <see cref="Subject{T}.OnCompleted"/>/<see cref="Subject{T}.OnError"/> is
    /// neither forwarded nor buffered, so it can never be wrongly replayed to a subscriber that arrives afterward.
    /// </summary>
    /// <param name="value">The value to buffer and push.</param>
    public override void OnNext(T value)
    {
        if (IsStopped || IsDisposed)
        {
            return;
        }

        lock (_bufferGate)
        {
            _buffer.Enqueue((_clock(), value));
            Trim();
        }

        base.OnNext(value);
    }

    /// <summary>
    /// Replays every currently buffered value to <paramref name="observer"/> (oldest first), trimming the buffer
    /// first, and then subscribes it to the underlying <see cref="Subject{T}"/> for subsequent live values exactly
    /// as <see cref="Subject{T}.Subscribe(IObserver{T})"/> would.
    /// </summary>
    /// <param name="observer">The observer to replay buffered values to and then subscribe.</param>
    /// <returns>A disposable that unsubscribes the observer from future (non-replayed) notifications.</returns>
    public override IDisposable Subscribe(IObserver<T> observer)
    {
        lock (_bufferGate)
        {
            Trim();
            foreach (var (_, value) in _buffer)
            {
                observer.OnNext(value);
            }
        }

        return base.Subscribe(observer);
    }

    /// <summary>Clears the replay buffer in addition to the base <see cref="Subject{T}"/> disposal behavior.</summary>
    public override void Dispose()
    {
        lock (_bufferGate)
        {
            _buffer.Clear();
        }

        base.Dispose();
    }

    private void Trim()
    {
        while (_buffer.Count > _bufferSize)
        {
            _buffer.Dequeue();
        }

        if (_windowTime is not { } window)
        {
            return;
        }

        var threshold = _clock() - window;
        while (_buffer.Count > 0 && _buffer.Peek().Timestamp < threshold)
        {
            _buffer.Dequeue();
        }
    }
}
