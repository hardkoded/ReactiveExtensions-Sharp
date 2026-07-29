namespace ReactiveExtensionsSharp;

/// <summary>
/// A value tagged with the time elapsed since the previous emission (or since subscription, for the first
/// emission). Produced by <see cref="Operators.TimeIntervalOperator.TimeInterval{T}"/>.
/// </summary>
/// <remarks>
/// Uses <see cref="IScheduler.Now"/> rather than reading the system clock directly, matching every other
/// time-based operator in this port, so a future virtual-time scheduler can control it in tests.
/// </remarks>
/// <typeparam name="T">The type of the tagged value.</typeparam>
public readonly struct TimeInterval<T> : IEquatable<TimeInterval<T>>
{
    /// <summary>Initializes a new instance of the <see cref="TimeInterval{T}"/> struct.</summary>
    /// <param name="value">The emitted value.</param>
    /// <param name="interval">The time elapsed since the previous emission, or since subscription for the first.</param>
    public TimeInterval(T value, TimeSpan interval)
    {
        Value = value;
        Interval = interval;
    }

    /// <summary>Gets the emitted value.</summary>
    public T Value { get; }

    /// <summary>Gets the time elapsed since the previous emission, or since subscription for the first emission.</summary>
    public TimeSpan Interval { get; }

    /// <inheritdoc/>
    public bool Equals(TimeInterval<T> other)
        => EqualityComparer<T>.Default.Equals(Value, other.Value) && Interval.Equals(other.Interval);

    /// <inheritdoc/>
    public override bool Equals(object? obj) => obj is TimeInterval<T> other && Equals(other);

    /// <inheritdoc/>
    public override int GetHashCode()
    {
        unchecked
        {
            var hash = 17;
            hash = (hash * 31) + (Value is null ? 0 : EqualityComparer<T>.Default.GetHashCode(Value));
            hash = (hash * 31) + Interval.GetHashCode();
            return hash;
        }
    }

    /// <inheritdoc/>
    public override string ToString() => $"TimeInterval({Value}, {Interval})";
}
