using RxSharp.Subjects;

namespace RxSharp.Operators;

/// <summary>The <c>groupBy</c> operator. See the main overload below for full behavior.</summary>
public static class GroupByOperator
{
    /// <summary>Groups source values by <paramref name="keySelector"/>, one <see cref="IGroupedObservable{TKey, T}"/> per distinct key.</summary>
    /// <typeparam name="T">The element type of the source.</typeparam>
    /// <typeparam name="TKey">The type of the shared key.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="keySelector">Selects the grouping key for each value.</param>
    /// <returns>An observable of groups, one per distinct key.</returns>
    public static Observable<IGroupedObservable<TKey, T>> GroupBy<T, TKey>(this Observable<T> source, Func<T, TKey> keySelector)
        where TKey : notnull
        => source.GroupBy(keySelector, value => value, (Func<IGroupedObservable<TKey, T>, Observable<Unit>>?)null);

    /// <summary>Groups source values by <paramref name="keySelector"/>, projecting each value with <paramref name="elementSelector"/> before it's added to its group.</summary>
    /// <typeparam name="T">The element type of the source.</typeparam>
    /// <typeparam name="TKey">The type of the shared key.</typeparam>
    /// <typeparam name="TElement">The type of the projected group elements.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="keySelector">Selects the grouping key for each value.</param>
    /// <param name="elementSelector">Projects each source value into the value emitted by its group.</param>
    /// <returns>An observable of groups, one per distinct key.</returns>
    public static Observable<IGroupedObservable<TKey, TElement>> GroupBy<T, TKey, TElement>(
        this Observable<T> source,
        Func<T, TKey> keySelector,
        Func<T, TElement> elementSelector)
        where TKey : notnull
        => source.GroupBy(keySelector, elementSelector, (Func<IGroupedObservable<TKey, TElement>, Observable<Unit>>?)null);

    /// <summary>
    /// Groups source values by <paramref name="keySelector"/> into one <see cref="IGroupedObservable{TKey, TElement}"/>
    /// per distinct key. If <paramref name="durationSelector"/> is given, it is subscribed once per newly created
    /// group; the group closes (completes, and a later value with the same key starts a brand-new group) as soon
    /// as that duration observable emits or completes.
    /// A callback failure in any of <paramref name="keySelector"/>/<paramref name="elementSelector"/>/
    /// <paramref name="durationSelector"/> is treated as fatal for the whole operation (matches rxjs): it errors the
    /// outer subscriber AND every currently open group, not just the group that happened to trigger it.
    /// Deliberately does not implement rxjs's ref-counted "close the source once the outer and every inner group
    /// have been unsubscribed" behavior — disposing the outer subscription always tears down the source directly.
    /// </summary>
    /// <typeparam name="T">The element type of the source.</typeparam>
    /// <typeparam name="TKey">The type of the shared key.</typeparam>
    /// <typeparam name="TElement">The type of the projected group elements.</typeparam>
    /// <typeparam name="TDuration">The (unused) element type of the duration observable.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="keySelector">Selects the grouping key for each value.</param>
    /// <param name="elementSelector">Projects each source value into the value emitted by its group.</param>
    /// <param name="durationSelector">Given a newly created group, returns an observable whose first emission or completion closes that group.</param>
    /// <returns>An observable of groups, one per distinct key (and per duration-selector-bounded lifetime, if given).</returns>
    public static Observable<IGroupedObservable<TKey, TElement>> GroupBy<T, TKey, TElement, TDuration>(
        this Observable<T> source,
        Func<T, TKey> keySelector,
        Func<T, TElement> elementSelector,
        Func<IGroupedObservable<TKey, TElement>, Observable<TDuration>>? durationSelector)
        where TKey : notnull
        => source.Operate<T, IGroupedObservable<TKey, TElement>>((src, subscriber) =>
        {
            var groups = new Dictionary<TKey, Subject<TElement>>();

            void ErrorAll(Exception error)
            {
                foreach (var group in groups.Values.ToArray())
                {
                    group.OnError(error);
                }

                subscriber.OnError(error);
            }

            return src.Subscribe(
                onNext: value =>
                {
                    TKey key;
                    try
                    {
                        key = keySelector(value);
                    }
                    catch (Exception ex)
                    {
                        ErrorAll(ex);
                        return;
                    }

                    if (!groups.TryGetValue(key, out var group))
                    {
                        group = new Subject<TElement>();
                        groups[key] = group;
                        var groupedObservable = new GroupedObservable<TKey, TElement>(key, group);

                        if (durationSelector is not null)
                        {
                            Observable<TDuration> duration;
                            try
                            {
                                duration = durationSelector(groupedObservable);
                            }
                            catch (Exception ex)
                            {
                                ErrorAll(ex);
                                return;
                            }

                            var durationSubscription = new SingleAssignmentDisposable();

                            void CloseGroup()
                            {
                                if (groups.Remove(key))
                                {
                                    group.OnCompleted();
                                }

                                subscriber.Remove(durationSubscription);
                                durationSubscription.Dispose();
                            }

                            durationSubscription.Disposable = duration.Subscribe(
                                onNext: _ => CloseGroup(),
                                onError: ErrorAll,
                                onComplete: CloseGroup);
                            subscriber.Add(durationSubscription);
                        }

                        subscriber.OnNext(groupedObservable);
                    }

                    TElement element;
                    try
                    {
                        element = elementSelector(value);
                    }
                    catch (Exception ex)
                    {
                        ErrorAll(ex);
                        return;
                    }

                    group.OnNext(element);
                },
                onError: ErrorAll,
                onComplete: () =>
                {
                    foreach (var group in groups.Values.ToArray())
                    {
                        group.OnCompleted();
                    }

                    subscriber.OnCompleted();
                });
        });
}
