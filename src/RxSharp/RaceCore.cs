namespace RxSharp;

/// <summary>Shared "first to emit wins" subscription logic used by <c>Observable.Race</c> and the <c>RaceWith</c> operator.</summary>
internal static class RaceCore
{
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
