namespace RxSharp.Operators;

/// <summary>Implements the <c>SequenceEqual</c> operator. Mirrors rxjs's <c>sequenceEqual</c>.</summary>
public static class SequenceEqualOperator
{
    /// <summary>
    /// Emits a single boolean indicating whether <paramref name="source"/> and <paramref name="other"/> emit
    /// exactly the same sequence of values, in the same order, and complete at the same point &#8212; using
    /// <paramref name="comparer"/> (or the default equality comparer for <typeparamref name="T"/>) to compare
    /// each pair of values.
    /// </summary>
    /// <typeparam name="T">The element type shared by both observables.</typeparam>
    /// <param name="source">The first observable to compare.</param>
    /// <param name="other">The second observable to compare against.</param>
    /// <param name="comparer">The comparer used to test each pair of values for equality. Defaults to <see cref="EqualityComparer{T}.Default"/> when <see langword="null"/>.</param>
    /// <returns>An observable that emits a single boolean, then completes.</returns>
    public static Observable<bool> SequenceEqual<T>(this Observable<T> source, Observable<T> other, IEqualityComparer<T>? comparer = null)
    {
        var equalityComparer = comparer ?? EqualityComparer<T>.Default;
        return source.SequenceEqual(other, equalityComparer.Equals);
    }

    /// <summary>
    /// Emits a single boolean indicating whether <paramref name="source"/> and <paramref name="other"/> emit
    /// exactly the same sequence of values, in the same order, and complete at the same point &#8212; using
    /// <paramref name="comparer"/> to compare each pair of values.
    /// </summary>
    /// <remarks>
    /// Both sources are buffered independently: a value arriving on one side is compared against the oldest
    /// unconsumed value from the other side if one is already waiting, or is itself buffered to wait for the
    /// other side to catch up otherwise. A mismatch, or a value arriving after the other side has already
    /// completed, emits <see langword="false"/> immediately and tears down both subscriptions &#8212; without
    /// waiting for either source to finish. If both sides complete having matched every buffered value,
    /// the result emits <see langword="true"/>. If <paramref name="comparer"/> throws, the exception is
    /// forwarded via <c>OnError</c> and both source subscriptions are torn down.
    /// </remarks>
    /// <typeparam name="T">The element type shared by both observables.</typeparam>
    /// <param name="source">The first observable to compare.</param>
    /// <param name="other">The second observable to compare against.</param>
    /// <param name="comparer">A function that compares a pair of values for equality.</param>
    /// <returns>An observable that emits a single boolean, then completes.</returns>
    public static Observable<bool> SequenceEqual<T>(this Observable<T> source, Observable<T> other, Func<T, T, bool> comparer)
        => source.Operate<T, bool>((src, subscriber) =>
        {
            var sourceState = new SequenceState<T>();
            var otherState = new SequenceState<T>();

            // Built directly (see Take.cs for the full explanation) so a match/mismatch found from within a
            // nested/synchronous onNext callback can dispose both sides' subscriptions immediately.
            Subscriber<T> sourceSubscriber = null!;
            Subscriber<T> otherSubscriber = null!;

            void Emit(bool isEqual)
            {
                subscriber.OnNext(isEqual);
                subscriber.OnCompleted();
                sourceSubscriber.Dispose();
                otherSubscriber.Dispose();
            }

            void HandleNext(SequenceState<T> self, SequenceState<T> peer, T value)
            {
                if (peer.Buffer.Count == 0)
                {
                    if (peer.IsComplete)
                    {
                        // The peer already finished, so this is an extra, unmatched value.
                        Emit(false);
                    }
                    else
                    {
                        self.Buffer.Enqueue(value);
                    }

                    return;
                }

                var peerValue = peer.Buffer.Dequeue();
                bool equal;
                try
                {
                    equal = comparer(value, peerValue);
                }
                catch (Exception ex)
                {
                    subscriber.OnError(ex);
                    return;
                }

                if (!equal)
                {
                    Emit(false);
                }
            }

            sourceSubscriber = Subscriber.Create<T>(
                onNext: value => HandleNext(sourceState, otherState, value),
                onError: subscriber.OnError,
                onComplete: () =>
                {
                    sourceState.IsComplete = true;
                    if (otherState.IsComplete)
                    {
                        Emit(otherState.Buffer.Count == 0);
                    }

                    sourceSubscriber.Dispose();
                });

            otherSubscriber = Subscriber.Create<T>(
                onNext: value => HandleNext(otherState, sourceState, value),
                onError: subscriber.OnError,
                onComplete: () =>
                {
                    otherState.IsComplete = true;
                    if (sourceState.IsComplete)
                    {
                        Emit(sourceState.Buffer.Count == 0);
                    }

                    otherSubscriber.Dispose();
                });

            var sourceSubscription = src.Subscribe(sourceSubscriber);
            var otherSubscription = other.Subscribe(otherSubscriber);

            return new Subscription(() =>
            {
                sourceSubscription.Dispose();
                otherSubscription.Dispose();
            });
        });

    /// <summary>Per-side buffering state tracked while comparing two sequences. Mirrors rxjs's internal <c>SequenceState</c>.</summary>
    /// <typeparam name="T">The element type of the buffered values.</typeparam>
    private sealed class SequenceState<T>
    {
        /// <summary>Values emitted on this side that have not yet been matched against the other side.</summary>
        public Queue<T> Buffer { get; } = new Queue<T>();

        /// <summary>Whether this side has completed.</summary>
        public bool IsComplete { get; set; }
    }
}
