namespace RxSharp;

/// <summary>Thrown by operators such as <c>First</c> when a source completes without emitting a value and no default was supplied. Mirrors rxjs's <c>EmptyError</c>.</summary>
public sealed class EmptyErrorException : Exception
{
    public EmptyErrorException()
        : base("The source observable was empty.")
    {
    }

    public EmptyErrorException(string message) : base(message)
    {
    }
}
