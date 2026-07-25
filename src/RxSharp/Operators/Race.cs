namespace RxSharp.Operators;

public static class RaceOperator
{
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
