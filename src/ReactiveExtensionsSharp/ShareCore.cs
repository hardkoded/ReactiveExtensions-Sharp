using ReactiveExtensionsSharp.Subjects;

namespace ReactiveExtensionsSharp;

/// <summary>
/// Shared multicast/reset subscription logic behind the <c>Share</c> and <c>ShareReplay</c> operators. Connects
/// to the source on the first subscriber. Each of the three reset triggers (source error, source completion,
/// subscriber count dropping back to zero) is independently configurable via a plain <see cref="bool"/> -- this
/// port doesn't implement rxjs's reset-notifier-observable knobs, just the common boolean case (see
/// <see cref="ReactiveExtensionsSharp.Operators.ShareConfig{T}"/>'s remarks for why).
/// </summary>
internal static class ShareCore
{
    /// <remarks>
    /// <paramref name="resetOnComplete"/> is what distinguishes rxjs's two default configs: <c>share()</c>
    /// defaults it <c>true</c> (a completed source goes fully cold again -- a later subscriber gets a fresh
    /// connector and resubscribes to the source), while <c>shareReplay()</c> defaults it to <c>false</c> (a
    /// completed source's buffered values keep replaying to every later subscriber forever, without ever
    /// resubscribing -- the caching behavior shareReplay is used for).
    /// </remarks>
    public static Observable<T> Multicast<T>(
        Observable<T> source,
        Func<Subject<T>> connectorFactory,
        bool resetOnError,
        bool resetOnComplete,
        bool resetOnRefCountZero)
    {
        var gate = new object();
        Subject<T>? connector = null;
        IDisposable? sourceSubscription = null;
        var refCount = 0;
        var hasCompleted = false;
        var hasErrored = false;

        void Reset()
        {
            connector = null;
            sourceSubscription = null;
            hasCompleted = false;
            hasErrored = false;
        }

        return new Observable<T>(subscriber =>
        {
            lock (gate)
            {
                refCount++;
                connector ??= connectorFactory();
                var activeConnector = connector;

                // Attach this subscriber to the connector *before* possibly connecting to the source below, so
                // a source that emits synchronously upon subscribe (e.g. Of(...)) still reaches it.
                var connectorSubscription = activeConnector.Subscribe(subscriber);

                // Wire this subscriber's own cleanup *directly onto it* -- via subscriber.Add rather than by
                // returning it for Observable<T>.Subscribe to attach afterwards -- so it's already registered
                // before source.Subscribe is called below. That matters when the source is a hand-rolled,
                // synchronous, self-checking observable (loops while !subscriber.IsDisposed): a downstream
                // operator like Take can dispose this very subscriber *from within* that synchronous call, and
                // without this early registration the cleanup wouldn't run until after the whole synchronous
                // call already unwound -- too late to stop the source from emitting further values.
                subscriber.Add(() =>
                {
                    lock (gate)
                    {
                        connectorSubscription.Dispose();
                        refCount--;

                        // Only a plain unsubscribe-driven drop to zero goes through this reset path -- if the
                        // source already errored/completed, that terminal event's own resetOnError/resetOnComplete
                        // handling already decided whether to reset, and resetOnRefCountZero must not re-decide it.
                        if (refCount == 0 && !hasCompleted && !hasErrored && resetOnRefCountZero)
                        {
                            var teardown = sourceSubscription;
                            Reset();
                            teardown?.Dispose();
                        }
                    }
                });

                // sourceSubscription being non-null covers both "still actively connected" and "connected once,
                // then the source completed/errored but we were told not to reset" -- either way, don't connect
                // again. Only connect when nothing is (or ever was, since the last reset) hooked up.
                if (sourceSubscription is null)
                {
                    // Built directly (rather than via the Subscribe(onNext:...) convenience overload) so this
                    // closure holds a reference to the inner subscriber *before* calling source.Subscribe --
                    // required for the same reason as subscriber.Add above: a synchronous source may reenter
                    // (e.g. a reentrant subscribe to this very shared observable) before Subscribe returns, and
                    // sourceSubscription must already be non-null by then so that reentrant call doesn't try to
                    // connect to the source a second time.
                    Subscriber<T> innerSubscriber = null!;
                    innerSubscriber = Subscriber.Create<T>(
                        onNext: activeConnector.OnNext,
                        onError: err =>
                        {
                            lock (gate)
                            {
                                hasErrored = true;
                            }

                            activeConnector.OnError(err);

                            if (resetOnError)
                            {
                                lock (gate)
                                {
                                    Reset();
                                }
                            }
                        },
                        onComplete: () =>
                        {
                            lock (gate)
                            {
                                hasCompleted = true;
                            }

                            activeConnector.OnCompleted();

                            if (resetOnComplete)
                            {
                                lock (gate)
                                {
                                    Reset();
                                }
                            }
                        });

                    sourceSubscription = innerSubscriber;
                    source.Subscribe(innerSubscriber);
                }
            }

            return null;
        });
    }
}
