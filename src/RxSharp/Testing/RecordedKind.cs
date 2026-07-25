namespace RxSharp.Testing;

/// <summary>The kind of notification captured by a <see cref="Recorded{T}"/>. Mirrors rxjs's <c>NotificationKind</c>.</summary>
public enum RecordedKind
{
    OnNext,
    OnError,
    OnCompleted,
}
