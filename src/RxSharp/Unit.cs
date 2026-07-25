namespace RxSharp;

/// <summary>Stands in for rxjs's <c>never</c> type parameter (e.g. streams that only ever error or complete).</summary>
public readonly struct Unit : IEquatable<Unit>
{
    public static readonly Unit Default = default;

    public bool Equals(Unit other) => true;

    public override bool Equals(object? obj) => obj is Unit;

    public override int GetHashCode() => 0;

    public override string ToString() => "()";
}
