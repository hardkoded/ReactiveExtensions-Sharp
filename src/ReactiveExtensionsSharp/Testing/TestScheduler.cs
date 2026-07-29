using ReactiveExtensionsSharp.Subjects;

namespace ReactiveExtensionsSharp.Testing;

/// <summary>
/// A <see cref="VirtualTimeScheduler"/> that additionally understands rxjs-style ASCII marble diagrams, so
/// time-based operator tests can build deterministic cold/hot sources and assert exactly when values, errors,
/// and completion happen — no real timers, no flakiness. See <see cref="MarbleParser"/> for the (deliberately
/// simplified) diagram grammar this supports.
/// </summary>
public class TestScheduler : VirtualTimeScheduler
{
    /// <summary>
    /// Finds the completion marker <c>|</c> in a plain time diagram (e.g. <c>"---|"</c>) and returns its frame
    /// as a <see cref="TimeSpan"/>, for use with <see cref="VirtualTimeScheduler.AdvanceTo"/>. Mirrors rxjs's
    /// <c>createTime</c>.
    /// </summary>
    /// <param name="marbles">A time diagram containing exactly one completion marker <c>|</c>.</param>
    /// <returns>The virtual time of the completion marker.</returns>
    public static TimeSpan ParseTime(string marbles)
    {
        if (marbles is null)
        {
            throw new ArgumentNullException(nameof(marbles));
        }

        var index = marbles.IndexOf('|');
        if (index == -1)
        {
            throw new ArgumentException("A time marble diagram must contain a completion marker '|'.", nameof(marbles));
        }

        return TimeSpan.FromTicks(index);
    }

    /// <summary>
    /// Builds a "cold" source: each independent subscription replays <paramref name="marbles"/> from its own
    /// subscription instant (frame 0 of the diagram = the moment <c>Subscribe</c> is called), same as rxjs's
    /// <c>cold()</c>. The diagram cannot contain a subscription marker <c>^</c> — a cold source has no fixed
    /// subscription point, every subscriber gets its own independent playback.
    /// </summary>
    /// <typeparam name="T">The element type of the observable.</typeparam>
    /// <param name="marbles">The marble diagram (see <see cref="MarbleParser"/> for the supported grammar).</param>
    /// <param name="values">Maps marble characters to emitted values; when omitted, the marble character itself is used as the value (requires <typeparamref name="T"/> to be <see cref="char"/>).</param>
    /// <param name="error">The error to emit for an <c>#</c> marker, if the diagram contains one; defaults to a generic exception.</param>
    /// <returns>An observable that replays the diagram independently for each subscriber.</returns>
    public Observable<T> CreateColdObservable<T>(string marbles, IReadOnlyDictionary<char, T>? values = null, Exception? error = null)
    {
        if (marbles is null)
        {
            throw new ArgumentNullException(nameof(marbles));
        }

        if (marbles.Any(c => c == '^'))
        {
            throw new ArgumentException(
                "A cold observable's marble diagram cannot contain a subscription marker '^' — every subscription starts its own independent playback.",
                nameof(marbles));
        }

        var messages = MarbleParser.Parse(marbles, values, error);

        return new Observable<T>(subscriber =>
        {
            var subscription = new Subscription();
            foreach (var message in messages)
            {
                subscription.Add(Schedule(() => Emit(subscriber, message), message.Time));
            }

            return subscription;
        });
    }

    /// <summary>
    /// Builds a "hot" source: a shared <see cref="Subject{T}"/> whose messages are scheduled once, at absolute
    /// virtual times measured from <see cref="VirtualTimeScheduler.Clock"/> at the moment this method is called
    /// (frame 0 of the diagram = that instant) — same as rxjs's <c>hot()</c>. Call this before advancing the
    /// clock unless you deliberately want the diagram's frames offset from the scheduler's current time.
    /// Subscribers that attach after a message's due time has already passed simply miss it, same as subscribing
    /// to a real <see cref="Subject{T}"/> late.
    /// </summary>
    /// <typeparam name="T">The element type of the observable.</typeparam>
    /// <param name="marbles">The marble diagram (see <see cref="MarbleParser"/> for the supported grammar).</param>
    /// <param name="values">Maps marble characters to emitted values; when omitted, the marble character itself is used as the value (requires <typeparamref name="T"/> to be <see cref="char"/>).</param>
    /// <param name="error">The error to emit for an <c>#</c> marker, if the diagram contains one; defaults to a generic exception.</param>
    /// <returns>An observable shared by every subscriber, driven by a single underlying <see cref="Subject{T}"/>.</returns>
    public Observable<T> CreateHotObservable<T>(string marbles, IReadOnlyDictionary<char, T>? values = null, Exception? error = null)
    {
        var messages = MarbleParser.Parse(marbles, values, error);
        var subject = new Subject<T>();
        var baseline = Clock;

        foreach (var message in messages)
        {
            var due = message.Time - baseline;
            Schedule(() => Emit(subject, message), due < TimeSpan.Zero ? TimeSpan.Zero : due);
        }

        return subject.AsObservable();
    }

    /// <summary>
    /// Subscribes to <paramref name="source"/> immediately and returns a list that fills in with a
    /// <see cref="Recorded{T}"/> (stamped with <see cref="VirtualTimeScheduler.Clock"/> at the moment each
    /// notification arrives) for every next/error/complete the source produces. Advance the scheduler afterward
    /// (<see cref="VirtualTimeScheduler.Start"/>, <see cref="VirtualTimeScheduler.AdvanceTo"/>, or
    /// <see cref="VirtualTimeScheduler.AdvanceBy"/>), then assert against the same list reference.
    /// </summary>
    /// <typeparam name="T">The element type of the observable.</typeparam>
    /// <param name="source">The observable to subscribe to and record notifications from.</param>
    /// <returns>A live list that fills in with each notification as the scheduler is advanced.</returns>
    public IReadOnlyList<Recorded<T>> Record<T>(Observable<T> source)
    {
        if (source is null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        var results = new List<Recorded<T>>();
        source.Subscribe(
            onNext: value => results.Add(Recorded.OnNext(Clock, value)),
            onError: error => results.Add(Recorded.OnError<T>(Clock, error)),
            onComplete: () => results.Add(Recorded.OnCompleted<T>(Clock)));
        return results;
    }

    private static void Emit<T>(IObserver<T> observer, Recorded<T> message)
    {
        switch (message.Kind)
        {
            case RecordedKind.OnNext:
                observer.OnNext(message.Value!);
                break;

            case RecordedKind.OnError:
                observer.OnError(message.Error!);
                break;

            case RecordedKind.OnCompleted:
                observer.OnCompleted();
                break;
        }
    }
}
