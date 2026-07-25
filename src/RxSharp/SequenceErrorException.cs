namespace RxSharp;

/// <summary>
/// Thrown by <c>Single</c> when more than one value (or more than one predicate match) is seen from the source.
/// Mirrors rxjs's <c>SequenceError</c>.
/// </summary>
public sealed class SequenceErrorException : Exception
{
    /// <summary>Initializes a new instance of the <see cref="SequenceErrorException"/> class with the default message.</summary>
    public SequenceErrorException()
        : base("Too many matching values")
    {
    }

    /// <summary>Initializes a new instance of the <see cref="SequenceErrorException"/> class with a custom message.</summary>
    /// <param name="message">The exception message.</param>
    public SequenceErrorException(string message)
        : base(message)
    {
    }
}
