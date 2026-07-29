using ReactiveExtensionsSharp.Subjects;

namespace ReactiveExtensionsSharp.Operators;

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

            // Subscribed via SubscribeChild since this is the single, stable subscription to `source` for the
            // whole lifetime of the operator (never replaced) — see OperatorHelper.SubscribeChild's doc comment.
            // This lets a downstream disposal (e.g. Take on the outer stream of groups) cascade up and stop a
            // fully-synchronous source mid-loop, instead of only once the whole synchronous call stack unwinds.
            return src.SubscribeChild(
                subscriber,
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

                            // Built directly and registered as a child of `subscriber` *before* subscribing (see
                            // OperatorHelper.SubscribeChild's doc comment for the general reasoning), then
                            // Remove()'d as soon as the group closes: groups — and therefore duration notifiers —
                            // are per-key and can be unbounded in number, so leaving each one in `subscriber`'s
                            // finalizer list forever would leak. Building it directly (rather than reassigning a
                            // SingleAssignmentDisposable after Subscribe returns) also avoids the
                            // WindowWhen/BufferWhen-style hazard (see CLAUDE.md) of a duration notifier that
                            // emits and then completes synchronously double-closing the same group.
                            Subscriber<TDuration> durationSubscriber = null!;

                            void CloseGroup()
                            {
                                subscriber.Remove(durationSubscriber);
                                durationSubscriber.Dispose();

                                if (groups.Remove(key))
                                {
                                    group.OnCompleted();
                                }
                            }

                            durationSubscriber = Subscriber.Create<TDuration>(
                                onNext: _ => CloseGroup(),
                                onError: ErrorAll,
                                onComplete: CloseGroup);

                            subscriber.Add(durationSubscriber);
                            duration.Subscribe(durationSubscriber);
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
