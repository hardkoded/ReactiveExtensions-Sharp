namespace ReactiveExtensionsSharp;

/// <summary>
/// Thrown by <c>Single</c> when the source emits one or more values but none of them satisfy the supplied
/// predicate. Mirrors rxjs's <c>NotFoundError</c>.
/// </summary>
public sealed class NotFoundErrorException : Exception
{
    /// <summary>Initializes a new instance of the <see cref="NotFoundErrorException"/> class with the default message.</summary>
    public NotFoundErrorException()
        : base("No matching values")
    {
    }

    /// <summary>Initializes a new instance of the <see cref="NotFoundErrorException"/> class with a custom message.</summary>
    /// <param name="message">The exception message.</param>
    public NotFoundErrorException(string message)
        : base(message)
    {
    }
}
