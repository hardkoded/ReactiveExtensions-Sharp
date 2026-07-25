namespace RxSharp.Testing;

/// <summary>
/// A single timestamped notification (next/error/complete), stamped with a <see cref="VirtualTimeScheduler"/>'s
/// clock value at the moment it happened. The core assertion unit of marble testing: a test records a sequence
/// of these from a live subscription (see <see cref="TestScheduler.Record{T}"/>), advances the scheduler, then
/// compares the recorded sequence against an expected one built from these same factory methods (or parsed from
/// a marble diagram, whose messages are also <see cref="Recorded{T}"/> values).
/// </summary>
public readonly struct Recorded<T> : IEquatable<Recorded<T>>
{
    internal Recorded(TimeSpan time, RecordedKind kind, T? value, Exception? error)
    {
        Time = time;
        Kind = kind;
        Value = value;
        Error = error;
    }

    public TimeSpan Time { get; }

    public RecordedKind Kind { get; }

    public T? Value { get; }

    public Exception? Error { get; }

    public bool Equals(Recorded<T> other)
    {
        if (Time != other.Time || Kind != other.Kind)
        {
            return false;
        }

        return Kind switch
        {
            RecordedKind.OnNext => EqualityComparer<T>.Default.Equals(Value!, other.Value!),
            RecordedKind.OnError => ErrorsMatch(Error, other.Error),
            _ => true,
        };
    }

    public override bool Equals(object? obj) => obj is Recorded<T> other && Equals(other);

    public override int GetHashCode()
    {
        unchecked
        {
            var hash = 17;
            hash = (hash * 31) + Time.GetHashCode();
            hash = (hash * 31) + Kind.GetHashCode();
            if (Kind == RecordedKind.OnNext && Value is not null)
            {
                hash = (hash * 31) + Value.GetHashCode();
            }

            return hash;
        }
    }

    public override string ToString() => Kind switch
    {
        RecordedKind.OnNext => $"{Time.Ticks}: OnNext({Value})",
        RecordedKind.OnError => $"{Time.Ticks}: OnError({Error?.GetType().Name}: {Error?.Message})",
        _ => $"{Time.Ticks}: OnCompleted()",
    };

    private static bool ErrorsMatch(Exception? left, Exception? right)
    {
        if (ReferenceEquals(left, right))
        {
            return true;
        }

        if (left is null || right is null)
        {
            return false;
        }

        return left.GetType() == right.GetType() && left.Message == right.Message;
    }
}

/// <summary>Factory helpers for <see cref="Recorded{T}"/>. A non-generic sibling avoids CA1000 (static members on generic types).</summary>
public static class Recorded
{
    public static Recorded<T> OnNext<T>(TimeSpan time, T value) => new Recorded<T>(time, RecordedKind.OnNext, value, null);

    public static Recorded<T> OnError<T>(TimeSpan time, Exception error) => new Recorded<T>(time, RecordedKind.OnError, default, error);

    public static Recorded<T> OnCompleted<T>(TimeSpan time) => new Recorded<T>(time, RecordedKind.OnCompleted, default, null);
}
