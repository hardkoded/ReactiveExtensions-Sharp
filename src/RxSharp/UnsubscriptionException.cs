namespace RxSharp;

/// <summary>Raised when one or more finalizers throw while a <see cref="Subscription"/> is being disposed. Mirrors rxjs's <c>UnsubscriptionError</c>.</summary>
public sealed class UnsubscriptionException : Exception
{
    public UnsubscriptionException()
    {
        Errors = Array.Empty<Exception>();
    }

    public UnsubscriptionException(IReadOnlyList<Exception> errors)
        : base($"{errors.Count} error(s) occurred during unsubscription.")
    {
        Errors = errors;
    }

    public IReadOnlyList<Exception> Errors { get; }
}
