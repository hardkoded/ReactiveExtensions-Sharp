namespace RxSharp;

/// <summary>Shared "first to emit wins" subscription logic used by <c>Observable.Race</c> and the <c>RaceWith</c> operator.</summary>
internal static class RaceCore
{
    /// <summary>Subscribes to every source in <paramref name="sources"/>; the first to emit "wins" and every other source's subscription is disposed at that point.</summary>
    /// <typeparam name="T">The element type shared by every source.</typeparam>
    /// <param name="sources">The candidate sources to race.</param>
    /// <param name="subscriber">The downstream subscriber to forward the winning source's notifications to.</param>
    /// <returns>A disposable that unsubscribes from every remaining source subscription.</returns>
    public static IDisposable Subscribe<T>(IReadOnlyList<Observable<T>> sources, Subscriber<T> subscriber)
    {
        var subscriptions = new List<IDisposable>();
        var won = false;

        for (var i = 0; i < sources.Count && !won && !subscriber.IsDisposed; i++)
        {
            var index = i;
            var sourceSubscription = new SingleAssignmentDisposable();
            subscriptions.Add(sourceSubscription);

            sourceSubscription.Disposable = sources[index].Subscribe(
                onNext: value =>
                {
                    if (!won)
                    {
                        won = true;
                        for (var s = 0; s < subscriptions.Count; s++)
                        {
                            if (s != index)
                            {
                                subscriptions[s].Dispose();
                            }
                        }
                    }

                    subscriber.OnNext(value);
                },
                onError: subscriber.OnError,
                onComplete: subscriber.OnCompleted);
        }

        return new Subscription(() =>
        {
            foreach (var subscription in subscriptions)
            {
                subscription.Dispose();
            }
        });
    }
}
