namespace ReactiveExtensionsSharp;

/// <summary>Thrown by operators such as <c>First</c> when a source completes without emitting a value and no default was supplied. Mirrors rxjs's <c>EmptyError</c>.</summary>
public sealed class EmptyErrorException : Exception
{
    /// <summary>Initializes a new instance of the <see cref="EmptyErrorException"/> class with the default message.</summary>
    public EmptyErrorException()
        : base("The source observable was empty.")
    {
    }

    /// <summary>Initializes a new instance of the <see cref="EmptyErrorException"/> class with a custom message.</summary>
    /// <param name="message">The exception message.</param>
    public EmptyErrorException(string message) : base(message)
    {
    }
}
