namespace RxSharp.Operators;

/// <summary>Implements the <c>IgnoreElements</c> operator. Mirrors rxjs's <c>ignoreElements</c>.</summary>
public static class IgnoreElementsOperator
{
    /// <summary>
    /// Suppresses every value emitted by <paramref name="source"/>, forwarding only its completion or error
    /// notification.
    /// </summary>
    /// <remarks>Useful when only the termination of <paramref name="source"/> matters, not the values it produces.</remarks>
    /// <typeparam name="T">The element type of the source observable.</typeparam>
    /// <param name="source">The source observable whose values should be ignored.</param>
    /// <returns>An observable that never emits a value, but completes or errors exactly when <paramref name="source"/> does.</returns>
    public static Observable<T> IgnoreElements<T>(this Observable<T> source)
        => source.Operate<T, T>((src, subscriber) => src.SubscribeChild(subscriber, onError: subscriber.OnError, onComplete: subscriber.OnCompleted));
}
