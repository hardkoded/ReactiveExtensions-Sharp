namespace ReactiveExtensionsSharp;

/// <summary>Stands in for rxjs's <c>never</c> type parameter (e.g. streams that only ever error or complete).</summary>
public readonly struct Unit : IEquatable<Unit>
{
    /// <summary>Gets the single value of the <see cref="Unit"/> type.</summary>
    public static readonly Unit Default = default;

    /// <summary>Determines whether this instance equals <paramref name="other"/>. Always <see langword="true"/>, since every <see cref="Unit"/> value is equivalent.</summary>
    /// <param name="other">The other value to compare against.</param>
    /// <returns>Always <see langword="true"/>.</returns>
    public bool Equals(Unit other) => true;

    /// <summary>Determines whether <paramref name="obj"/> is a <see cref="Unit"/> value.</summary>
    /// <param name="obj">The object to compare against.</param>
    /// <returns><see langword="true"/> if <paramref name="obj"/> is a <see cref="Unit"/>; otherwise, <see langword="false"/>.</returns>
    public override bool Equals(object? obj) => obj is Unit;

    /// <summary>Returns a constant hash code, since every <see cref="Unit"/> value is equivalent.</summary>
    /// <returns>Always <c>0</c>.</returns>
    public override int GetHashCode() => 0;

    /// <summary>Returns a string representation of this value.</summary>
    /// <returns>The literal string <c>"()"</c>.</returns>
    public override string ToString() => "()";
}
