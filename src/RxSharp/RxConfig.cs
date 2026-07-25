namespace RxSharp;

/// <summary>
/// Global, mutable configuration hooks. Mirrors rxjs's <c>config</c> module.
/// </summary>
public static class RxConfig
{
    private static readonly Action<Exception> DefaultUnhandledErrorHandler =
        error => ThreadPool.QueueUserWorkItem(_ => throw error);

    /// <summary>
    /// Invoked when an error reaches a subscriber with no error handler. Deliberately does not
    /// rethrow on the current (synchronous) call stack: doing so would be caught by an enclosing
    /// operator's own <c>Subscribe</c> call and swallowed instead of surfacing to the caller. The
    /// default schedules the throw on the thread pool so it still surfaces as an unhandled exception.
    /// </summary>
    public static Action<Exception> OnUnhandledError { get; set; } = DefaultUnhandledErrorHandler;

    public static void ResetOnUnhandledError() => OnUnhandledError = DefaultUnhandledErrorHandler;
}
