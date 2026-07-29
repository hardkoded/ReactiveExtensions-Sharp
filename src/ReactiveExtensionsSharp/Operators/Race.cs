namespace ReactiveExtensionsSharp.Operators;

/// <summary>Extension methods implementing the <c>raceWith</c> operator.</summary>
public static class RaceOperator
{
    /// <summary>
    /// Subscribes to <paramref name="source"/> and every observable in <paramref name="otherSources"/> at the
    /// same time. Whichever one produces the first notification of any kind (a value, an error, or completion)
    /// "wins" the race: the output mirrors that observable from then on, and every other, losing subscription
    /// is unsubscribed immediately, so a losing source can never emit or run further side effects afterward.
    /// </summary>
    /// <typeparam name="T">The type of values shared by <paramref name="source"/> and <paramref name="otherSources"/>.</typeparam>
    /// <param name="source">The source sequence, entered into the race alongside <paramref name="otherSources"/>.</param>
    /// <param name="otherSources">The other sequences to race against <paramref name="source"/>.</param>
    /// <returns>
    /// An observable that mirrors whichever of <paramref name="source"/> or <paramref name="otherSources"/>
    /// emits its first notification first. If <paramref name="otherSources"/> is empty, <paramref name="source"/>
    /// itself is returned unchanged.
    /// </returns>
    public static Observable<T> RaceWith<T>(this Observable<T> source, params Observable<T>[] otherSources)
    {
        if (otherSources.Length == 0)
        {
            return source;
        }

        var sources = new Observable<T>[otherSources.Length + 1];
        sources[0] = source;
        Array.Copy(otherSources, 0, sources, 1, otherSources.Length);

        return source.Operate<T, T>((_, subscriber) => RaceCore.Subscribe(sources, subscriber));
    }
}
