using RxSharp.Subjects;

namespace RxSharp;

/// <summary>
/// Shared multicast/reset subscription logic behind the <c>Share</c> and <c>ShareReplay</c> operators. Connects
/// to the source on the first subscriber. Whenever the subscriber count drops back to zero purely from
/// unsubscription (the source itself hasn't errored or completed), the connector is dropped and a later
/// subscription reconnects to the source from scratch -- this is rxjs's <c>resetOnRefCountZero: true</c>, always
/// on here (this port doesn't implement the reset-notifier-observable knobs, just the common boolean case).
/// </summary>
internal static class ShareCore
{
    /// <remarks>
    /// <paramref name="resetOnComplete"/> is the one behavioral knob exposed to callers, because it's what
    /// actually distinguishes rxjs's two operators: <c>share()</c> defaults <c>resetOnComplete: true</c> (a
    /// completed source goes fully cold again -- a later subscriber gets a fresh connector and resubscribes to
    /// the source), while <c>shareReplay()</c> defaults it to <c>false</c> (a completed source's buffered values
    /// keep replaying to every later subscriber forever, without ever resubscribing -- the caching behavior
    /// shareReplay is used for). <c>resetOnError</c> is always <c>true</c> for both operators here (matches
    /// rxjs's default for both), since neither operator needs it configurable.
    /// </remarks>
    public static Observable<T> Multicast<T>(Observable<T> source, Func<Subject<T>> connectorFactory, bool resetOnComplete)
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
                        if (refCount == 0 && !hasCompleted && !hasErrored)
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

                            lock (gate)
                            {
                                Reset();
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
