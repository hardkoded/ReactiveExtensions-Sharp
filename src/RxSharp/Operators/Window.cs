using RxSharp.Subjects;

namespace RxSharp.Operators;

/// <summary>The <c>window</c> operator.</summary>
public static class WindowOperator
{
    /// <summary>
    /// Branches out <paramref name="source"/>'s values as a nested <see cref="Observable{T}"/> that opens
    /// immediately and closes (completing) each time <paramref name="boundary"/> emits, opening a new one right
    /// away. Mirrors rxjs's <c>window</c>: <paramref name="boundary"/> is subscribed exactly once, for the whole
    /// lifetime of the output — unlike <see cref="WindowWhenOperator.WindowWhen{T, TBoundary}"/>, it is not
    /// re-subscribed per window. <paramref name="boundary"/> completing does not close (or otherwise affect) the
    /// current window or the output — it simply means no further window boundaries will occur.
    /// </summary>
    /// <typeparam name="T">The element type of the source.</typeparam>
    /// <typeparam name="TBoundary">The (unused) element type of <paramref name="boundary"/>.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="boundary">Each emission closes the current window and opens a new one.</param>
    /// <returns>An observable of windows, each itself an observable of the values collected during its lifetime.</returns>
    public static Observable<Observable<T>> Window<T, TBoundary>(this Observable<T> source, Observable<TBoundary> boundary)
        => source.Operate<T, Observable<T>>((src, subscriber) =>
        {
            var window = new Subject<T>();
            subscriber.OnNext(window.AsObservable());

            void HandleError(Exception error)
            {
                window.OnError(error);
                subscriber.OnError(error);
            }

            src.SubscribeChild(
                subscriber,
                onNext: value => window.OnNext(value),
                onError: HandleError,
                onComplete: () =>
                {
                    window.OnCompleted();
                    subscriber.OnCompleted();
                });

            boundary.SubscribeChild(
                subscriber,
                onNext: _ =>
                {
                    window.OnCompleted();
                    window = new Subject<T>();
                    subscriber.OnNext(window.AsObservable());
                },
                onError: HandleError);

            return null;
        });
}
