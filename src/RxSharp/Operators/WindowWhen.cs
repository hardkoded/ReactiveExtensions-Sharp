using RxSharp.Subjects;

namespace RxSharp.Operators;

/// <summary>The <c>windowWhen</c> operator.</summary>
public static class WindowWhenOperator
{
    /// <summary>
    /// Branches out <paramref name="source"/>'s values as a nested <see cref="Observable{T}"/> that closes
    /// (completing) and immediately reopens each time the observable returned by <paramref name="closingSelector"/>
    /// emits or completes. Mirrors rxjs's <c>windowWhen</c>. The first window opens eagerly at subscription time;
    /// <paramref name="closingSelector"/> is invoked again, and its result subscribed to afresh, every time a new
    /// window opens.
    /// </summary>
    /// <remarks>
    /// Each closing-notifier subscription is torn down as soon as it produces its first signal (next or complete) —
    /// built directly via <see cref="Subscriber.Create{T}"/> assigned to its slot <em>before</em> subscribing (the
    /// same pattern <c>Take</c>/<c>First</c> use — see their remarks), rather than a <see cref="SingleAssignmentDisposable"/>
    /// reassigned only after <c>Subscribe</c> returns. This matters here specifically: a closing notifier that emits
    /// and then completes synchronously (e.g. <c>Observable.Of(1)</c>) would otherwise reopen the window twice for a
    /// single notifier, since both the synchronous <c>next</c> and the following <c>complete</c> would each trigger a
    /// reopen before the naive wrapper-reassignment could take effect.
    /// </remarks>
    /// <typeparam name="T">The element type of the source.</typeparam>
    /// <typeparam name="TBoundary">The (unused) element type of the closing-notifier observable.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="closingSelector">Invoked (with no arguments) each time a window opens; the observable it returns closes that window on its first next or complete notification.</param>
    /// <returns>An observable of windows, each itself an observable of the values collected during its lifetime.</returns>
    public static Observable<Observable<T>> WindowWhen<T, TBoundary>(this Observable<T> source, Func<Observable<TBoundary>> closingSelector)
        => source.Operate<T, Observable<T>>((src, subscriber) =>
        {
            Subject<T>? window = null;
            Subscriber<TBoundary>? closingSubscriber = null;

            void HandleError(Exception error)
            {
                window?.OnError(error);
                subscriber.OnError(error);
            }

            void OpenWindow()
            {
                closingSubscriber?.Dispose();
                window?.OnCompleted();

                window = new Subject<T>();
                subscriber.OnNext(window.AsObservable());

                Observable<TBoundary> closingNotifier;
                try
                {
                    closingNotifier = closingSelector();
                }
                catch (Exception ex)
                {
                    HandleError(ex);
                    return;
                }

                closingSubscriber = Subscriber.Create<TBoundary>(
                    onNext: _ => OpenWindow(),
                    onError: HandleError,
                    onComplete: OpenWindow);
                closingNotifier.Subscribe(closingSubscriber);
            }

            OpenWindow();

            var sourceSubscription = src.Subscribe(
                onNext: value => window?.OnNext(value),
                onError: HandleError,
                onComplete: () =>
                {
                    window?.OnCompleted();
                    subscriber.OnCompleted();
                });
            subscriber.Add(sourceSubscription);

            return new Subscription(() => closingSubscriber?.Dispose());
        });
}
