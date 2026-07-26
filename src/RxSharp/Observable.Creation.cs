namespace RxSharp;

/// <summary>Creation functions for <see cref="Observable{T}"/>. Mirrors rxjs's <c>observable/</c> creation functions.</summary>
public static class Observable
{
    /// <summary>Creates an <see cref="Observable{T}"/> that synchronously emits each of the given <paramref name="values"/>, in order, then completes.</summary>
    /// <typeparam name="T">The type of the emitted values.</typeparam>
    /// <param name="values">The values to emit.</param>
    /// <returns>An observable that emits each value, then completes.</returns>
    public static Observable<T> Of<T>(params T[] values) => From(values);

    /// <summary>Creates an <see cref="Observable{T}"/> that synchronously emits every item of <paramref name="values"/>, in enumeration order, then completes.</summary>
    /// <typeparam name="T">The type of the emitted values.</typeparam>
    /// <param name="values">The sequence to emit values from.</param>
    /// <returns>An observable that emits every item of <paramref name="values"/>, then completes.</returns>
    public static Observable<T> From<T>(IEnumerable<T> values) => new Observable<T>(subscriber =>
    {
        foreach (var value in values)
        {
            if (subscriber.IsDisposed)
            {
                return;
            }

            subscriber.OnNext(value);
        }

        subscriber.OnCompleted();
    });

    /// <summary>
    /// Creates an <see cref="Observable{T}"/> that emits the result of <paramref name="task"/> and completes,
    /// or errors if the task faults or is cancelled. Continuation runs on the default task scheduler, not
    /// synchronously on the subscribing thread.
    /// </summary>
    /// <typeparam name="T">The type of the task's result.</typeparam>
    /// <param name="task">The task to await.</param>
    /// <returns>An observable that emits the task's result once it completes.</returns>
    public static Observable<T> From<T>(Task<T> task) => new Observable<T>(subscriber =>
    {
        task.ContinueWith(
            completed =>
            {
                if (subscriber.IsDisposed)
                {
                    return;
                }

                if (completed.IsFaulted)
                {
                    subscriber.OnError(completed.Exception!.GetBaseException());
                }
                else if (completed.IsCanceled)
                {
                    subscriber.OnError(new TaskCanceledException(completed));
                }
                else
                {
                    subscriber.OnNext(completed.Result);
                    subscriber.OnCompleted();
                }
            },
            TaskScheduler.Default);
    });

    /// <summary>Creates an <see cref="Observable{T}"/> that calls <paramref name="factory"/> to produce a fresh source for each new subscriber, rather than sharing one source across subscribers.</summary>
    /// <typeparam name="T">The type of the emitted values.</typeparam>
    /// <param name="factory">Invoked once per subscription to produce the source observable.</param>
    /// <returns>An observable that defers creation of its source until subscribe time.</returns>
    public static Observable<T> Defer<T>(Func<Observable<T>> factory) => new Observable<T>(subscriber => factory().Subscribe(subscriber));

    /// <summary>
    /// Creates an <see cref="Observable{TSource}"/> that creates a resource via <paramref name="resourceFactory"/>
    /// at subscribe time, uses <paramref name="observableFactory"/> to build the actual source from that
    /// resource, and disposes the resource once the subscription ends — whichever of complete, error, or
    /// unsubscribe comes first. A fresh resource (and a fresh source) is created independently for every
    /// subscription; neither is shared across subscribers. Mirrors rxjs's <c>using</c>.
    /// </summary>
    /// <remarks>
    /// Deviates from rxjs's <c>using</c> in one respect: rxjs's resource factory may return nothing at all (no
    /// resource), and the "resource" only needs a structural <c>unsubscribe()</c> method. C# has no equivalent
    /// to an optional structural type here, so this overload requires <typeparamref name="TResource"/> to
    /// implement <see cref="IDisposable"/> directly — pass a resource whose <see cref="IDisposable.Dispose"/> is
    /// a no-op if no real cleanup is needed. If <paramref name="observableFactory"/> throws, the resource —
    /// already created by <paramref name="resourceFactory"/> by that point — is still disposed before the error
    /// is forwarded; if <paramref name="resourceFactory"/> itself throws, there is no resource to dispose and
    /// the error is simply forwarded (via the same synchronous-exception-to-<c>OnError</c> path every
    /// <see cref="Observable{T}"/> subscribe delegate already gets).
    /// </remarks>
    /// <typeparam name="TSource">The type of the emitted values.</typeparam>
    /// <typeparam name="TResource">The resource type, disposed when the subscription ends.</typeparam>
    /// <param name="resourceFactory">Invoked once per subscription to create the resource.</param>
    /// <param name="observableFactory">Invoked once per subscription, with the resource just created, to build the source observable.</param>
    /// <returns>An observable that mirrors the source built by <paramref name="observableFactory"/>, disposing the resource when the subscription ends.</returns>
    public static Observable<TSource> Using<TSource, TResource>(Func<TResource> resourceFactory, Func<TResource, Observable<TSource>> observableFactory)
        where TResource : IDisposable
        => new Observable<TSource>(subscriber =>
        {
            var resource = resourceFactory();

            Observable<TSource> source;
            try
            {
                source = observableFactory(resource);
            }
            catch (Exception ex)
            {
                resource.Dispose();
                subscriber.OnError(ex);
                return null;
            }

            source.Subscribe(subscriber);
            return new Subscription(() => resource.Dispose());
        });

    /// <summary>Creates an <see cref="Observable{T}"/> that completes immediately, without ever emitting a value.</summary>
    /// <typeparam name="T">The (unused) element type, needed only so the result can be composed with other <see cref="Observable{T}"/> sources of the same type.</typeparam>
    /// <returns>An observable that completes immediately.</returns>
    public static Observable<T> Empty<T>() => new Observable<T>(subscriber => subscriber.OnCompleted());

    /// <summary>Creates an <see cref="Observable{T}"/> that never emits, errors, or completes.</summary>
    /// <typeparam name="T">The (unused) element type, needed only so the result can be composed with other <see cref="Observable{T}"/> sources of the same type.</typeparam>
    /// <returns>An observable that never produces a notification.</returns>
    public static Observable<T> Never<T>() => new Observable<T>(_ => { });

    /// <summary>Creates an <see cref="Observable{T}"/> that immediately errors with the exception produced by <paramref name="errorFactory"/>, without ever emitting a value.</summary>
    /// <typeparam name="T">The (unused) element type, needed only so the result can be composed with other <see cref="Observable{T}"/> sources of the same type.</typeparam>
    /// <param name="errorFactory">Invoked once per subscription to produce the exception to error with.</param>
    /// <returns>An observable that errors immediately.</returns>
    public static Observable<T> ThrowError<T>(Func<Exception> errorFactory) => new Observable<T>(subscriber => subscriber.OnError(errorFactory()));

    /// <summary>Creates an <see cref="Observable{T}"/> that emits a single value (<c>0</c>) after <paramref name="dueTime"/> elapses, then completes.</summary>
    /// <param name="dueTime">How long to wait before emitting.</param>
    /// <param name="scheduler">The scheduler to use for timing; defaults to <see cref="TaskPoolScheduler.Instance"/> when <see langword="null"/>.</param>
    /// <returns>An observable that emits once after the given delay.</returns>
    public static Observable<long> Timer(TimeSpan dueTime, IScheduler? scheduler = null) => new Observable<long>(subscriber =>
    {
        var activeScheduler = scheduler ?? TaskPoolScheduler.Instance;
        return activeScheduler.Schedule(
            () =>
            {
                subscriber.OnNext(0L);
                subscriber.OnCompleted();
            },
            dueTime);
    });

    /// <summary>Creates an <c>Observable&lt;long&gt;</c> that emits an incrementing counter (starting at <c>0</c>) on every tick of a fixed <paramref name="period"/>, forever, until unsubscribed.</summary>
    /// <param name="period">The fixed period between ticks. A negative value is treated as zero.</param>
    /// <param name="scheduler">The scheduler to use for timing; defaults to <see cref="TaskPoolScheduler.Instance"/> when <see langword="null"/>.</param>
    /// <returns>An observable that emits <c>0, 1, 2, ...</c> on every tick of <paramref name="period"/>. Never completes on its own.</returns>
    public static Observable<long> Interval(TimeSpan period, IScheduler? scheduler = null) => new Observable<long>(subscriber =>
    {
        var activeScheduler = scheduler ?? TaskPoolScheduler.Instance;
        var count = 0L;
        var timerSubscription = new SingleAssignmentDisposable();

        void Tick()
        {
            if (subscriber.IsDisposed)
            {
                return;
            }

            subscriber.OnNext(count++);

            if (subscriber.IsDisposed)
            {
                return;
            }

            timerSubscription.Disposable = activeScheduler.Schedule(Tick, period);
        }

        timerSubscription.Disposable = activeScheduler.Schedule(Tick, period);
        return timerSubscription;
    });

    /// <summary>Creates an <c>Observable&lt;int&gt;</c> that synchronously emits <paramref name="count"/> sequential integers, starting at <paramref name="start"/>, then completes.</summary>
    /// <param name="start">The first value to emit.</param>
    /// <param name="count">How many values to emit. If zero or negative, no values are emitted and the observable completes immediately.</param>
    /// <returns>An observable that emits <paramref name="start"/>, <paramref name="start"/>+1, ..., <paramref name="start"/>+<paramref name="count"/>-1, then completes.</returns>
    public static Observable<int> Range(int start, int count) => new Observable<int>(subscriber =>
    {
        for (var i = 0; i < count; i++)
        {
            if (subscriber.IsDisposed)
            {
                return;
            }

            subscriber.OnNext(start + i);
        }

        subscriber.OnCompleted();
    });

    /// <summary>Creates an <c>Observable&lt;int&gt;</c> that synchronously emits <paramref name="count"/> sequential integers, starting at <c>0</c>, then completes. Equivalent to <see cref="Range(int, int)"/> with <c>start</c> of <c>0</c>.</summary>
    /// <param name="count">How many values to emit. If zero or negative, no values are emitted and the observable completes immediately.</param>
    /// <returns>An observable that emits <c>0, 1, ..., count-1</c>, then completes.</returns>
    public static Observable<int> Range(int count) => Range(0, count);

    /// <summary>
    /// Generates an <see cref="Observable{TResult}"/> by running a state-driven loop: while <paramref name="condition"/>
    /// holds for the current state, emits <paramref name="resultSelector"/> applied to that state, then advances the
    /// state via <paramref name="iterate"/>. Runs synchronously (like a <c>for</c> loop) when <paramref name="scheduler"/>
    /// is <see langword="null"/>, checking <c>IsDisposed</c> before each iteration; otherwise each step is scheduled
    /// one at a time via <paramref name="scheduler"/>, so the loop can be interleaved with other scheduled work
    /// and cancelled between steps.
    /// </summary>
    /// <typeparam name="TState">The type of the loop state.</typeparam>
    /// <typeparam name="TResult">The type of the emitted values.</typeparam>
    /// <param name="initialState">The state the loop starts with.</param>
    /// <param name="condition">Tested against the current state before each iteration; the loop stops (and the observable completes) once this returns <see langword="false"/>.</param>
    /// <param name="iterate">Advances the state after each emission.</param>
    /// <param name="resultSelector">Projects the current state into the value to emit for this iteration.</param>
    /// <param name="scheduler">If given, each loop step is scheduled on this scheduler instead of running synchronously.</param>
    /// <returns>An observable that emits one value per iteration of the loop, then completes. Errors if <paramref name="condition"/>, <paramref name="iterate"/>, or <paramref name="resultSelector"/> throws.</returns>
    public static Observable<TResult> Generate<TState, TResult>(
        TState initialState,
        Func<TState, bool> condition,
        Func<TState, TState> iterate,
        Func<TState, TResult> resultSelector,
        IScheduler? scheduler = null)
        => new Observable<TResult>(subscriber =>
        {
            if (scheduler is null)
            {
                var state = initialState;
                try
                {
                    while (condition(state))
                    {
                        if (subscriber.IsDisposed)
                        {
                            return null;
                        }

                        subscriber.OnNext(resultSelector(state));

                        if (subscriber.IsDisposed)
                        {
                            return null;
                        }

                        state = iterate(state);
                    }
                }
                catch (Exception ex)
                {
                    subscriber.OnError(ex);
                    return null;
                }

                subscriber.OnCompleted();
                return null;
            }

            var current = initialState;
            var stepSubscription = new SingleAssignmentDisposable();

            void Step()
            {
                bool shouldContinue;
                try
                {
                    shouldContinue = condition(current);
                }
                catch (Exception ex)
                {
                    subscriber.OnError(ex);
                    return;
                }

                if (!shouldContinue)
                {
                    subscriber.OnCompleted();
                    return;
                }

                try
                {
                    subscriber.OnNext(resultSelector(current));
                    current = iterate(current);
                }
                catch (Exception ex)
                {
                    subscriber.OnError(ex);
                    return;
                }

                if (!subscriber.IsDisposed)
                {
                    stepSubscription.Disposable = scheduler.Schedule(Step, TimeSpan.Zero);
                }
            }

            stepSubscription.Disposable = scheduler.Schedule(Step, TimeSpan.Zero);
            return stepSubscription;
        });

    /// <summary>Overload of <see cref="Generate{TState, TResult}(TState, Func{TState, bool}, Func{TState, TState}, Func{TState, TResult}, IScheduler)"/> with no <c>resultSelector</c> — the state itself is emitted at each iteration.</summary>
    /// <typeparam name="TState">The type of the loop state, and of the emitted values.</typeparam>
    /// <param name="initialState">The state the loop starts with.</param>
    /// <param name="condition">Tested against the current state before each iteration; the loop stops once this returns <see langword="false"/>.</param>
    /// <param name="iterate">Advances the state after each emission.</param>
    /// <param name="scheduler">If given, each loop step is scheduled on this scheduler instead of running synchronously.</param>
    /// <returns>An observable that emits the state itself at each iteration, then completes.</returns>
    public static Observable<TState> Generate<TState>(TState initialState, Func<TState, bool> condition, Func<TState, TState> iterate, IScheduler? scheduler = null)
        => Generate(initialState, condition, iterate, Identity, scheduler);

    /// <summary>
    /// Overload of <see cref="Generate{TState, TResult}(TState, Func{TState, bool}, Func{TState, TState}, Func{TState, TResult}, IScheduler)"/>
    /// with no <c>condition</c> and no <c>resultSelector</c> — the loop never stops on its own and the state itself is
    /// emitted (pair with e.g. <c>Take</c> downstream). Deliberately not overloaded further to accept a <c>resultSelector</c>
    /// without a <c>condition</c> too: with generic <c>Func</c> parameters of matching arity, C# overload resolution
    /// cannot always tell such an overload apart from this one (e.g. a <c>throw</c>-expression lambda, whose inferred
    /// return type is ambiguous) — pass <c>condition: _ => true</c> to the full overload instead if a custom
    /// <c>resultSelector</c> is needed on an otherwise-infinite generator.
    /// </summary>
    /// <typeparam name="TState">The type of the loop state, and of the emitted values.</typeparam>
    /// <param name="initialState">The state the loop starts with.</param>
    /// <param name="iterate">Advances the state after each emission.</param>
    /// <param name="scheduler">If given, each loop step is scheduled on this scheduler instead of running synchronously.</param>
    /// <returns>An observable that emits the state itself at each iteration, forever.</returns>
    public static Observable<TState> Generate<TState>(TState initialState, Func<TState, TState> iterate, IScheduler? scheduler = null)
        => Generate(initialState, _ => true, iterate, Identity, scheduler);

    /// <summary>
    /// Creates an <see cref="Observable{T}"/> that, at subscribe time, evaluates <paramref name="condition"/> and
    /// subscribes to <paramref name="trueSource"/> if it returns <see langword="true"/>, or <paramref name="falseSource"/>
    /// otherwise. <paramref name="condition"/> is deliberately not evaluated until subscription (like <see cref="Defer{T}"/>,
    /// which this is implemented on top of), so the same <see cref="Observable{T}"/> instance can pick a different
    /// branch for each subscriber.
    /// </summary>
    /// <typeparam name="T">The element type shared by both branches.</typeparam>
    /// <param name="condition">Evaluated once per subscription to choose the branch.</param>
    /// <param name="trueSource">Subscribed to if <paramref name="condition"/> returns <see langword="true"/>.</param>
    /// <param name="falseSource">Subscribed to if <paramref name="condition"/> returns <see langword="false"/>.</param>
    /// <returns>An observable that mirrors whichever branch <paramref name="condition"/> selects.</returns>
    public static Observable<T> Iif<T>(Func<bool> condition, Observable<T> trueSource, Observable<T> falseSource)
        => Defer(() => condition() ? trueSource : falseSource);

    /// <summary>
    /// Creates an <see cref="Observable{T}"/> that subscribes to all <paramref name="sources"/> at once and mirrors
    /// whichever one emits, errors, or completes first; every other source is unsubscribed as soon as one "wins".
    /// </summary>
    /// <typeparam name="T">The type of the emitted values.</typeparam>
    /// <param name="sources">The candidate sources to race against each other.</param>
    /// <returns>An observable that mirrors the first source to produce a notification.</returns>
    public static Observable<T> Race<T>(params Observable<T>[] sources) => new Observable<T>(subscriber => RaceCore.Subscribe(sources, subscriber));

    /// <summary>Creates an <see cref="Observable{T}"/> that subscribes to each of <paramref name="sources"/> one at a time, in order, moving to the next only once the previous one completes.</summary>
    /// <typeparam name="T">The type of the emitted values.</typeparam>
    /// <param name="sources">The sources to subscribe to sequentially.</param>
    /// <returns>An observable that emits every source's values in sequence, completing after the last source completes.</returns>
    public static Observable<T> Concat<T>(params Observable<T>[] sources) => new Observable<T>(subscriber =>
    {
        var index = 0;

        void SubscribeNext()
        {
            if (index >= sources.Length)
            {
                subscriber.OnCompleted();
                return;
            }

            var next = sources[index++];
            var subscription = new SingleAssignmentDisposable();
            subscriber.Add(subscription);
            subscription.Disposable = next.Subscribe(onNext: subscriber.OnNext, onError: subscriber.OnError, onComplete: SubscribeNext);
        }

        SubscribeNext();
    });

    /// <summary>
    /// Creates an <see cref="Observable{T}"/> that subscribes to each of <paramref name="sources"/> in order,
    /// moving to the next source as soon as the current one either completes <em>or</em> errors — unlike
    /// <see cref="Concat{T}"/>, which stops (and forwards the error) the first time a source errors. The
    /// resulting observable completes once every source has been exhausted and never itself errors, no matter
    /// how many of the sources errored along the way.
    /// </summary>
    /// <remarks>
    /// This deliberately does not use the same "resubscribe as an unconditional teardown action" trick rxjs's
    /// own implementation uses internally (see rxjs 7.8.2's <c>observable/onErrorResumeNext.ts</c>): there, the
    /// per-source child subscriber resubscribes to the next source as one of its own registered teardown
    /// actions, which — because an external unsubscribe of the outer observable cascades down and disposes that
    /// same child subscriber — would also (incorrectly) advance to the next source on a plain external
    /// unsubscribe, not just on the current source's own completion or error. Here, moving to the next source is
    /// triggered directly from inside the current source's <c>onError</c>/<c>onComplete</c> callbacks only
    /// (matching the pattern <see cref="RxSharp.Operators.RetryOperator.Retry{T}"/> uses for its own per-attempt
    /// resubscription), so an external unsubscribe correctly stops the whole chain instead of skipping ahead.
    /// </remarks>
    /// <typeparam name="T">The type of the emitted values.</typeparam>
    /// <param name="sources">The sources to subscribe to sequentially. An empty array completes immediately, matching <see cref="Empty{T}"/>.</param>
    /// <returns>An observable that emits every source's values in sequence, moving past errors instead of forwarding them, completing once every source has ended.</returns>
    public static Observable<T> OnErrorResumeNext<T>(params Observable<T>[] sources) => new Observable<T>(subscriber =>
    {
        var index = 0;

        void SubscribeNext()
        {
            if (subscriber.IsDisposed)
            {
                return;
            }

            if (index >= sources.Length)
            {
                subscriber.OnCompleted();
                return;
            }

            var next = sources[index++];

            Subscriber<T> innerSubscriber = null!;
            innerSubscriber = Subscriber.Create<T>(
                onNext: subscriber.OnNext,
                onError: _ =>
                {
                    subscriber.Remove(innerSubscriber);
                    innerSubscriber.Dispose();
                    SubscribeNext();
                },
                onComplete: () =>
                {
                    subscriber.Remove(innerSubscriber);
                    innerSubscriber.Dispose();
                    SubscribeNext();
                });

            subscriber.Add(innerSubscriber);
            next.Subscribe(innerSubscriber);
        }

        SubscribeNext();
    });

    /// <summary>Creates an <see cref="Observable{T}"/> that subscribes to all <paramref name="sources"/> concurrently and emits every value from every source as it arrives, completing once all sources have completed.</summary>
    /// <typeparam name="T">The type of the emitted values.</typeparam>
    /// <param name="sources">The sources to merge together.</param>
    /// <returns>An observable that emits the interleaved values of every source.</returns>
    public static Observable<T> Merge<T>(params Observable<T>[] sources) => new Observable<T>(subscriber =>
    {
        if (sources.Length == 0)
        {
            subscriber.OnCompleted();
            return null;
        }

        var remaining = sources.Length;
        var subscriptions = new List<IDisposable>();

        foreach (var source in sources)
        {
            if (subscriber.IsDisposed)
            {
                break;
            }

            subscriptions.Add(source.Subscribe(
                onNext: subscriber.OnNext,
                onError: subscriber.OnError,
                onComplete: () =>
                {
                    remaining--;
                    if (remaining == 0)
                    {
                        subscriber.OnCompleted();
                    }
                }));
        }

        return new Subscription(() =>
        {
            foreach (var subscription in subscriptions)
            {
                subscription.Dispose();
            }
        });
    });

    /// <summary>
    /// Wraps a .NET event (add/remove handler pair) as an <see cref="Observable{T}"/>. Mirrors rxjs's
    /// <c>fromEvent</c> and doubles as the C# analogue of Puppeteer's own <c>fromEmitterEvent</c> helper —
    /// .NET events are a fundamentally different shape from JS EventEmitters/DOM targets, so rather than
    /// port rxjs's many duck-typed overloads, this follows the established Rx.NET <c>FromEvent</c> idiom.
    /// </summary>
    /// <typeparam name="TDelegate">The delegate type of the event handler (e.g. a custom event delegate type).</typeparam>
    /// <typeparam name="TEventArgs">The type of the event's payload, forwarded as each emitted value.</typeparam>
    /// <param name="addHandler">Called once, at subscribe time, with the handler to add to the event.</param>
    /// <param name="removeHandler">Called on unsubscribe, with the same handler, to remove it from the event.</param>
    /// <param name="conversion">Converts an <see cref="Action{TEventArgs}"/> callback into the event's actual delegate shape.</param>
    /// <returns>An observable that emits each event payload as it is raised. Never completes on its own; unsubscribing removes the underlying event handler.</returns>
    public static Observable<TEventArgs> FromEvent<TDelegate, TEventArgs>(
        Action<TDelegate> addHandler,
        Action<TDelegate> removeHandler,
        Func<Action<TEventArgs>, TDelegate> conversion)
        => new Observable<TEventArgs>(subscriber =>
        {
            var handler = conversion(subscriber.OnNext);
            addHandler(handler);
            return new Subscription(() => removeHandler(handler));
        });

    /// <summary>The common case of <see cref="FromEvent{TDelegate, TEventArgs}"/> for standard <see cref="EventHandler{TEventArgs}"/>-shaped .NET events.</summary>
    /// <typeparam name="TEventArgs">The type of the event's payload, forwarded as each emitted value.</typeparam>
    /// <param name="addHandler">Called once, at subscribe time, with the handler to add to the event.</param>
    /// <param name="removeHandler">Called on unsubscribe, with the same handler, to remove it from the event.</param>
    /// <returns>An observable that emits each event's <typeparamref name="TEventArgs"/> payload as it is raised.</returns>
    public static Observable<TEventArgs> FromEvent<TEventArgs>(Action<EventHandler<TEventArgs>> addHandler, Action<EventHandler<TEventArgs>> removeHandler)
        => FromEvent<EventHandler<TEventArgs>, TEventArgs>(addHandler, removeHandler, onNext => (_, args) => onNext(args));

    /// <summary>
    /// Waits for every source to emit at a given index, then emits the combined values as a list, positionally.
    /// Same-type-only for now (unlike rxjs's heterogeneously-typed tuple overloads) — C# has no variadic
    /// generics; add typed 2/3/4-arg overloads later if a real use case needs mixed element types.
    /// </summary>
    /// <typeparam name="T">The element type shared by every source.</typeparam>
    /// <param name="sources">The sources to zip together, positionally.</param>
    /// <returns>An observable that emits a list of the Nth value from every source, in order, completing once any source has completed and been fully drained.</returns>
    public static Observable<IReadOnlyList<T>> Zip<T>(params Observable<T>[] sources)
    {
        if (sources.Length == 0)
        {
            return Empty<IReadOnlyList<T>>();
        }

        return new Observable<IReadOnlyList<T>>(subscriber =>
        {
            var buffers = new List<Queue<T>>();
            for (var i = 0; i < sources.Length; i++)
            {
                buffers.Add(new Queue<T>());
            }

            var sourceCompleted = new bool[sources.Length];
            var subscriptions = new List<IDisposable>();

            void TryEmit()
            {
                while (buffers.TrueForAll(buffer => buffer.Count > 0))
                {
                    var combined = new List<T>(sources.Length);
                    foreach (var buffer in buffers)
                    {
                        combined.Add(buffer.Dequeue());
                    }

                    subscriber.OnNext(combined);
                }

                for (var i = 0; i < sources.Length; i++)
                {
                    if (sourceCompleted[i] && buffers[i].Count == 0)
                    {
                        subscriber.OnCompleted();
                        return;
                    }
                }
            }

            for (var i = 0; i < sources.Length; i++)
            {
                var index = i;
                subscriptions.Add(sources[index].Subscribe(
                    onNext: value =>
                    {
                        buffers[index].Enqueue(value);
                        TryEmit();
                    },
                    onError: subscriber.OnError,
                    onComplete: () =>
                    {
                        sourceCompleted[index] = true;
                        TryEmit();
                    }));
            }

            return new Subscription(() =>
            {
                foreach (var subscription in subscriptions)
                {
                    subscription.Dispose();
                }
            });
        });
    }

    /// <summary>Waits for every source to complete, then emits a list of each source's last value — but only if every source emitted at least one value. Same-type-only, see <see cref="Zip{T}"/>.</summary>
    /// <typeparam name="T">The element type shared by every source.</typeparam>
    /// <param name="sources">The sources to wait on.</param>
    /// <returns>An observable that emits one list of final values once every source has completed, or completes immediately without emitting if any source completed without a value.</returns>
    public static Observable<IReadOnlyList<T>> ForkJoin<T>(params Observable<T>[] sources)
    {
        if (sources.Length == 0)
        {
            return Empty<IReadOnlyList<T>>();
        }

        return new Observable<IReadOnlyList<T>>(subscriber =>
        {
            var values = new T[sources.Length];
            var hasValue = new bool[sources.Length];
            var remaining = sources.Length;
            var subscriptions = new List<IDisposable>();

            for (var i = 0; i < sources.Length; i++)
            {
                var index = i;
                subscriptions.Add(sources[index].Subscribe(
                    onNext: value =>
                    {
                        values[index] = value;
                        hasValue[index] = true;
                    },
                    onError: subscriber.OnError,
                    onComplete: () =>
                    {
                        if (!hasValue[index])
                        {
                            subscriber.OnCompleted();
                            return;
                        }

                        remaining--;
                        if (remaining == 0)
                        {
                            subscriber.OnNext(values);
                            subscriber.OnCompleted();
                        }
                    }));
            }

            return new Subscription(() =>
            {
                foreach (var subscription in subscriptions)
                {
                    subscription.Dispose();
                }
            });
        });
    }

    /// <summary>Emits a list of every source's latest value whenever any source emits, once all sources have emitted at least once. Same-type-only, see <see cref="Zip{T}"/>.</summary>
    /// <typeparam name="T">The element type shared by every source.</typeparam>
    /// <param name="sources">The sources to combine.</param>
    /// <returns>An observable that emits a snapshot list of the latest values, updated on every subsequent emission once every source has emitted at least once.</returns>
    public static Observable<IReadOnlyList<T>> CombineLatest<T>(params Observable<T>[] sources)
    {
        if (sources.Length == 0)
        {
            return Empty<IReadOnlyList<T>>();
        }

        return new Observable<IReadOnlyList<T>>(subscriber =>
        {
            var values = new T[sources.Length];
            var hasValue = new bool[sources.Length];
            var completed = new bool[sources.Length];
            var hasAllValues = false;
            var subscriptions = new List<IDisposable>();

            for (var i = 0; i < sources.Length; i++)
            {
                var index = i;
                subscriptions.Add(sources[index].Subscribe(
                    onNext: value =>
                    {
                        values[index] = value;
                        hasValue[index] = true;
                        if (!hasAllValues && Array.TrueForAll(hasValue, has => has))
                        {
                            hasAllValues = true;
                        }

                        if (hasAllValues)
                        {
                            subscriber.OnNext((T[])values.Clone());
                        }
                    },
                    onError: subscriber.OnError,
                    onComplete: () =>
                    {
                        completed[index] = true;
                        if (!hasValue[index] || Array.TrueForAll(completed, c => c))
                        {
                            subscriber.OnCompleted();
                        }
                    }));
            }

            return new Subscription(() =>
            {
                foreach (var subscription in subscriptions)
                {
                    subscription.Dispose();
                }
            });
        });
    }

    /// <summary>Returns <paramref name="value"/> unchanged. Useful as a default/no-op projection where an operator requires a selector function.</summary>
    /// <typeparam name="T">The type of the value.</typeparam>
    /// <param name="value">The value to return.</param>
    /// <returns><paramref name="value"/>, unchanged.</returns>
    public static T Identity<T>(T value) => value;

    /// <summary>Does nothing. Useful as a default callback where an operator requires a delegate (e.g. an ignored <c>onNext</c>/<c>onComplete</c> handler).</summary>
    public static void Noop()
    {
    }
}

