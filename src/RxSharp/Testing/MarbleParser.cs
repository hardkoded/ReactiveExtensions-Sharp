namespace RxSharp.Testing;

/// <summary>
/// Parses rxjs-style ASCII marble diagrams into timestamped <see cref="Recorded{T}"/> messages. Each character
/// is one virtual time frame (character index <c>i</c> maps to <see cref="TimeSpan.FromTicks(long)"/> of <c>i</c>),
/// matching <see cref="VirtualTimeScheduler"/>'s tick-based clock.
/// <para>
/// Deliberately simplified relative to rxjs's own parser: no grouped <c>()</c> syntax (each character always
/// occupies its own frame), no explicit unsubscription marker <c>!</c>, no time-progression literals. Supported
/// markers: <c>-</c> and space (nothing happens, just advances one frame), any other character (an emitted
/// value — resolved via the supplied dictionary, or used as a literal <c>char</c> value if no dictionary was
/// given and <c>T</c> is <see cref="char"/>), <c>|</c> (complete), <c>#</c> (error), and <c>^</c> (accepted and
/// ignored — documents a subscription point for a human reader without changing parsing behavior).
/// </para>
/// </summary>
internal static class MarbleParser
{
    public static IReadOnlyList<Recorded<T>> Parse<T>(string marbles, IReadOnlyDictionary<char, T>? values, Exception? error)
    {
        if (marbles is null)
        {
            throw new ArgumentNullException(nameof(marbles));
        }

        var messages = new List<Recorded<T>>();

        for (var i = 0; i < marbles.Length; i++)
        {
            var time = TimeSpan.FromTicks(i);
            switch (marbles[i])
            {
                case '-':
                case ' ':
                case '^':
                    break;

                case '|':
                    messages.Add(Recorded.OnCompleted<T>(time));
                    break;

                case '#':
                    messages.Add(Recorded.OnError<T>(time, error ?? new InvalidOperationException("Marble test error.")));
                    break;

                default:
                    messages.Add(Recorded.OnNext(time, ResolveValue(marbles[i], values)));
                    break;
            }
        }

        return messages;
    }

    private static T ResolveValue<T>(char marble, IReadOnlyDictionary<char, T>? values)
    {
        if (values is not null)
        {
            if (values.TryGetValue(marble, out var value))
            {
                return value;
            }

            throw new ArgumentException($"No value provided for marble '{marble}'.", nameof(values));
        }

        if (typeof(T) == typeof(char))
        {
            return (T)(object)marble;
        }

        throw new ArgumentException(
            $"No values dictionary was provided and {typeof(T)} is not char, so marble '{marble}' cannot be resolved to a value.",
            nameof(values));
    }
}
