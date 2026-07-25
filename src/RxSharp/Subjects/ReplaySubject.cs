namespace RxSharp.Subjects;

/// <summary>A <see cref="Subject{T}"/> that replays buffered values to new subscribers. Mirrors rxjs's <c>ReplaySubject</c>.</summary>
public sealed class ReplaySubject<T> : Subject<T>
{
    private readonly int _bufferSize;
    private readonly TimeSpan? _windowTime;
    private readonly Func<DateTimeOffset> _clock;
    private readonly Queue<(DateTimeOffset Timestamp, T Value)> _buffer = new Queue<(DateTimeOffset Timestamp, T Value)>();
    private readonly object _bufferGate = new object();

    public ReplaySubject(int bufferSize = int.MaxValue, TimeSpan? windowTime = null, Func<DateTimeOffset>? clock = null)
    {
        _bufferSize = bufferSize;
        _windowTime = windowTime;
        _clock = clock ?? (() => DateTimeOffset.UtcNow);
    }

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
