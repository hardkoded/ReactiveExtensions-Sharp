namespace ReactiveExtensionsSharp.Operators;

/// <summary>Implements the <c>IsEmpty</c> operator. Mirrors rxjs's <c>isEmpty</c>.</summary>
public static class IsEmptyOperator
{
    /// <summary>
    /// Emits a single boolean: <see langword="true"/> and completes if <paramref name="source"/> completes
    /// without ever emitting a value, or <see langword="false"/> and completes as soon as the first value
    /// arrives &#8212; without waiting for <paramref name="source"/> itself to complete.
    /// </summary>
    /// <typeparam name="T">The element type of the source observable.</typeparam>
    /// <param name="source">The source observable to check for emptiness.</param>
    /// <returns>An observable that emits a single boolean, then completes.</returns>
    public static Observable<bool> IsEmpty<T>(this Observable<T> source)
        => source.Operate<T, bool>((src, subscriber) =>
        {
            // Built directly (see Take.cs for the full explanation) so this closure can dispose the inner
            // subscriber immediately upon the first value, even from within a nested/synchronous source callback.
            Subscriber<T> innerSubscriber = null!;
            innerSubscriber = Subscriber.Create<T>(
                onNext: _ =>
                {
                    subscriber.OnNext(false);
                    subscriber.OnCompleted();
                    innerSubscriber.Dispose();
                },
                onError: subscriber.OnError,
                onComplete: () =>
                {
                    subscriber.OnNext(true);
                    subscriber.OnCompleted();
                });

            return src.Subscribe(innerSubscriber);
        });
}
