namespace ReactiveExtensionsSharp;

/// <summary>Identifies which kind of notification a <see cref="Notification{T}"/> represents.</summary>
public enum NotificationKind
{
    /// <summary>An <c>OnNext</c> notification, carrying a value.</summary>
    Next,

    /// <summary>An <c>OnError</c> notification, carrying an exception.</summary>
    Error,

    /// <summary>An <c>OnCompleted</c> notification.</summary>
    Completed,
}

/// <summary>
/// A materialized next/error/complete notification, represented as a plain, inert value instead of a live
/// callback invocation. Produced by <see cref="Operators.MaterializeOperator.Materialize{T}"/> (which turns a
/// stream's notifications into <see cref="Notification{T}"/> values flowing through the stream, so e.g. an
/// error becomes a value rather than terminating the stream) and consumed by
/// <see cref="Operators.MaterializeOperator.Dematerialize{T}"/> (which converts them back).
/// </summary>
/// <remarks>
/// This lives in the main <c>ReactiveExtensionsSharp</c> namespace, not <c>ReactiveExtensionsSharp.Testing</c>, since materialize/dematerialize
/// are core operators rather than test-only utilities. <c>ReactiveExtensionsSharp.Testing</c> has a superficially similar type,
/// <see cref="ReactiveExtensionsSharp.Testing.Recorded{T}"/>, but that type also carries a <see cref="ReactiveExtensionsSharp.Testing.VirtualTimeScheduler"/>
/// clock timestamp that has no meaning here, and core types should not depend on the testing namespace — so
/// this is a separate, deliberately simpler type rather than a reuse of <see cref="ReactiveExtensionsSharp.Testing.Recorded{T}"/>.
/// </remarks>
/// <typeparam name="T">The type of value carried by a <see cref="NotificationKind.Next"/> notification.</typeparam>
public readonly struct Notification<T> : IEquatable<Notification<T>>
{
    internal Notification(NotificationKind kind, T? value, Exception? error)
    {
        Kind = kind;
        Value = value;
        Error = error;
    }

    /// <summary>Gets which kind of notification this is.</summary>
    public NotificationKind Kind { get; }

    /// <summary>Gets the emitted value, for a <see cref="NotificationKind.Next"/> notification; otherwise the default value.</summary>
    public T? Value { get; }

    /// <summary>Gets the error, for a <see cref="NotificationKind.Error"/> notification; otherwise <see langword="null"/>.</summary>
    public Exception? Error { get; }

    /// <summary>Applies this notification to <paramref name="observer"/> by invoking the matching <see cref="IObserver{T}"/> method.</summary>
    /// <param name="observer">The observer to notify.</param>
    public void Accept(IObserver<T> observer)
    {
        switch (Kind)
        {
            case NotificationKind.Next:
                observer.OnNext(Value!);
                break;
            case NotificationKind.Error:
                observer.OnError(Error!);
                break;
            default:
                observer.OnCompleted();
                break;
        }
    }

    /// <summary>Determines whether this notification matches another (errors compare by type and message, not reference).</summary>
    /// <param name="other">The notification to compare against.</param>
    /// <returns><see langword="true"/> if the two notifications match.</returns>
    public bool Equals(Notification<T> other)
    {
        if (Kind != other.Kind)
        {
            return false;
        }

        return Kind switch
        {
            NotificationKind.Next => EqualityComparer<T>.Default.Equals(Value!, other.Value!),
            NotificationKind.Error => ErrorsMatch(Error, other.Error),
            _ => true,
        };
    }

    /// <inheritdoc/>
    public override bool Equals(object? obj) => obj is Notification<T> other && Equals(other);

    /// <inheritdoc/>
    public override int GetHashCode()
    {
        unchecked
        {
            var hash = 17;
            hash = (hash * 31) + Kind.GetHashCode();
            if (Kind == NotificationKind.Next && Value is not null)
            {
                hash = (hash * 31) + Value.GetHashCode();
            }

            return hash;
        }
    }

    /// <inheritdoc/>
    public override string ToString() => Kind switch
    {
        NotificationKind.Next => $"OnNext({Value})",
        NotificationKind.Error => $"OnError({Error?.GetType().Name}: {Error?.Message})",
        _ => "OnCompleted()",
    };

    private static bool ErrorsMatch(Exception? left, Exception? right)
    {
        if (ReferenceEquals(left, right))
        {
            return true;
        }

        if (left is null || right is null)
        {
            return false;
        }

        return left.GetType() == right.GetType() && left.Message == right.Message;
    }
}

/// <summary>Factory helpers for <see cref="Notification{T}"/>. A non-generic sibling avoids CA1000 (static members on generic types).</summary>
public static class Notification
{
    /// <summary>Creates a <see cref="NotificationKind.Next"/> notification carrying <paramref name="value"/>.</summary>
    /// <typeparam name="T">The type of the emitted value.</typeparam>
    /// <param name="value">The emitted value.</param>
    /// <returns>The notification.</returns>
    public static Notification<T> CreateNext<T>(T value) => new Notification<T>(NotificationKind.Next, value, null);

    /// <summary>Creates a <see cref="NotificationKind.Error"/> notification carrying <paramref name="error"/>.</summary>
    /// <typeparam name="T">The element type of the observable the error was notified on.</typeparam>
    /// <param name="error">The error.</param>
    /// <returns>The notification.</returns>
    public static Notification<T> CreateError<T>(Exception error) => new Notification<T>(NotificationKind.Error, default, error);

    /// <summary>Creates a <see cref="NotificationKind.Completed"/> notification.</summary>
    /// <typeparam name="T">The element type of the observable that completed.</typeparam>
    /// <returns>The notification.</returns>
    public static Notification<T> CreateCompleted<T>() => new Notification<T>(NotificationKind.Completed, default, null);
}
