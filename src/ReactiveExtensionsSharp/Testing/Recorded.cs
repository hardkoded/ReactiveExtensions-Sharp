namespace ReactiveExtensionsSharp.Testing;

/// <summary>
/// A single timestamped notification (next/error/complete), stamped with a <see cref="VirtualTimeScheduler"/>'s
/// clock value at the moment it happened. The core assertion unit of marble testing: a test records a sequence
/// of these from a live subscription (see <see cref="TestScheduler.Record{T}"/>), advances the scheduler, then
/// compares the recorded sequence against an expected one built from these same factory methods (or parsed from
/// a marble diagram, whose messages are also <see cref="Recorded{T}"/> values).
/// </summary>
/// <typeparam name="T">The type of value carried by an <see cref="RecordedKind.OnNext"/> notification.</typeparam>
public readonly struct Recorded<T> : IEquatable<Recorded<T>>
{
    internal Recorded(TimeSpan time, RecordedKind kind, T? value, Exception? error)
    {
        Time = time;
        Kind = kind;
        Value = value;
        Error = error;
    }

    /// <summary>Gets the virtual-clock time the notification was recorded at.</summary>
    public TimeSpan Time { get; }

    /// <summary>Gets which kind of notification this is.</summary>
    public RecordedKind Kind { get; }

    /// <summary>Gets the emitted value, for an <see cref="RecordedKind.OnNext"/> notification; otherwise the default value.</summary>
    public T? Value { get; }

    /// <summary>Gets the error, for an <see cref="RecordedKind.OnError"/> notification; otherwise <see langword="null"/>.</summary>
    public Exception? Error { get; }

    /// <summary>Determines whether this notification matches another (errors compare by type and message, not reference).</summary>
    /// <param name="other">The notification to compare against.</param>
    /// <returns><see langword="true"/> if the two notifications match.</returns>
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

    /// <inheritdoc/>
    public override bool Equals(object? obj) => obj is Recorded<T> other && Equals(other);

    /// <inheritdoc/>
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

    /// <inheritdoc/>
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
    /// <summary>Creates a recorded <c>OnNext</c> notification at the given virtual time.</summary>
    /// <typeparam name="T">The type of the emitted value.</typeparam>
    /// <param name="time">The virtual-clock time the value was emitted at.</param>
    /// <param name="value">The emitted value.</param>
    /// <returns>The recorded notification.</returns>
    public static Recorded<T> OnNext<T>(TimeSpan time, T value) => new Recorded<T>(time, RecordedKind.OnNext, value, null);

    /// <summary>Creates a recorded <c>OnError</c> notification at the given virtual time.</summary>
    /// <typeparam name="T">The element type of the observable the error was recorded on.</typeparam>
    /// <param name="time">The virtual-clock time the error occurred at.</param>
    /// <param name="error">The error.</param>
    /// <returns>The recorded notification.</returns>
    public static Recorded<T> OnError<T>(TimeSpan time, Exception error) => new Recorded<T>(time, RecordedKind.OnError, default, error);

    /// <summary>Creates a recorded <c>OnCompleted</c> notification at the given virtual time.</summary>
    /// <typeparam name="T">The element type of the observable that completed.</typeparam>
    /// <param name="time">The virtual-clock time completion occurred at.</param>
    /// <returns>The recorded notification.</returns>
    public static Recorded<T> OnCompleted<T>(TimeSpan time) => new Recorded<T>(time, RecordedKind.OnCompleted, default, null);
}
