namespace ReactiveExtensionsSharp;

/// <summary>
/// A value tagged with the wall-clock time it was received. Produced by
/// <see cref="Operators.TimestampOperator.Timestamp{T}"/>.
/// </summary>
/// <remarks>
/// Uses <see cref="IScheduler.Now"/> rather than reading the system clock directly, matching every other
/// time-based operator in this port, so a future virtual-time scheduler can control it in tests.
/// The timestamp property is named <see cref="TimestampValue"/>, not <c>Timestamp</c> (rxjs's own field name),
/// because C# (CS0542) forbids a member from sharing its enclosing type's name.
/// </remarks>
/// <typeparam name="T">The type of the tagged value.</typeparam>
public readonly struct Timestamp<T> : IEquatable<Timestamp<T>>
{
    /// <summary>Initializes a new instance of the <see cref="Timestamp{T}"/> struct.</summary>
    /// <param name="value">The emitted value.</param>
    /// <param name="timestamp">The time the value was received.</param>
    public Timestamp(T value, DateTimeOffset timestamp)
    {
        Value = value;
        TimestampValue = timestamp;
    }

    /// <summary>Gets the emitted value.</summary>
    public T Value { get; }

    /// <summary>Gets the time <see cref="Value"/> was received.</summary>
    public DateTimeOffset TimestampValue { get; }

    /// <inheritdoc/>
    public bool Equals(Timestamp<T> other)
        => EqualityComparer<T>.Default.Equals(Value, other.Value) && TimestampValue.Equals(other.TimestampValue);

    /// <inheritdoc/>
    public override bool Equals(object? obj) => obj is Timestamp<T> other && Equals(other);

    /// <inheritdoc/>
    public override int GetHashCode()
    {
        unchecked
        {
            var hash = 17;
            hash = (hash * 31) + (Value is null ? 0 : EqualityComparer<T>.Default.GetHashCode(Value));
            hash = (hash * 31) + TimestampValue.GetHashCode();
            return hash;
        }
    }

    /// <inheritdoc/>
    public override string ToString() => $"Timestamp({Value}, {TimestampValue})";
}
