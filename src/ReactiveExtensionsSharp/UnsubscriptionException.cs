namespace ReactiveExtensionsSharp;

/// <summary>Raised when one or more finalizers throw while a <see cref="Subscription"/> is being disposed. Mirrors rxjs's <c>UnsubscriptionError</c>.</summary>
public sealed class UnsubscriptionException : Exception
{
    /// <summary>Initializes a new instance of the <see cref="UnsubscriptionException"/> class with no inner errors.</summary>
    public UnsubscriptionException()
    {
        Errors = Array.Empty<Exception>();
    }

    /// <summary>Initializes a new instance of the <see cref="UnsubscriptionException"/> class wrapping the given finalizer errors.</summary>
    /// <param name="errors">The exceptions thrown by individual finalizers during disposal.</param>
    public UnsubscriptionException(IReadOnlyList<Exception> errors)
        : base($"{errors.Count} error(s) occurred during unsubscription.")
    {
        Errors = errors;
    }

    /// <summary>Gets the exceptions thrown by individual finalizers while a <see cref="Subscription"/> was being disposed.</summary>
    public IReadOnlyList<Exception> Errors { get; }
}
