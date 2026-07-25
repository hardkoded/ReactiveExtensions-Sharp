namespace RxSharp.Operators;

/// <summary>Implements the <c>Skip</c> operator. Mirrors rxjs's <c>skip</c>.</summary>
public static class SkipOperator
{
    /// <summary>
    /// Skips the first <paramref name="count"/> values from <paramref name="source"/>, then emits every value
    /// after that unchanged.
    /// </summary>
    /// <remarks>
    /// If <paramref name="source"/> emits fewer than <paramref name="count"/> values before completing, the
    /// result simply completes without ever emitting anything.
    /// </remarks>
    /// <typeparam name="T">The element type of the source observable.</typeparam>
    /// <param name="source">The source observable to skip values from.</param>
    /// <param name="count">The number of leading values to skip. If zero or negative, no values are skipped.</param>
    /// <returns>An observable of the values from <paramref name="source"/> after the first <paramref name="count"/> are skipped.</returns>
    public static Observable<T> Skip<T>(this Observable<T> source, int count)
    {
        if (count <= 0)
        {
            return source;
        }

        return source.Operate<T, T>((src, subscriber) =>
        {
            var seen = 0;

            // Built directly (see Take.cs for the full explanation) rather than via the Subscribe(onNext:...)
            // convenience overload. Skip never completes early on its own, but it must still cooperate with a
            // downstream operator that does (e.g. Take): forwarding a value can synchronously trigger that
            // downstream operator's own early-completion logic, which disposes *its* subscription -- here, that
            // is exactly this `subscriber` parameter, since it is passed through unwrapped when it is already a
            // Subscriber<T>. Checking subscriber.IsDisposed right after forwarding and cascading the dispose to
            // our own inner subscriber is what lets a synchronous, self-checking source stop looping immediately
            // instead of only after the whole chain unwinds.
            Subscriber<T> innerSubscriber = null!;
            innerSubscriber = Subscriber.Create<T>(
                onNext: value =>
                {
                    if (seen < count)
                    {
                        seen++;
                    }
                    else
                    {
                        subscriber.OnNext(value);
                    }

                    if (subscriber.IsDisposed)
                    {
                        innerSubscriber.Dispose();
                    }
                },
                onError: subscriber.OnError,
                onComplete: subscriber.OnCompleted);

            return src.Subscribe(innerSubscriber);
        });
    }
}
