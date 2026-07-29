namespace ReactiveExtensionsSharp.Testing;

/// <summary>The kind of notification captured by a <see cref="Recorded{T}"/>. Mirrors rxjs's <c>NotificationKind</c>.</summary>
public enum RecordedKind
{
    /// <summary>A recorded <c>OnNext</c> notification.</summary>
    OnNext,

    /// <summary>A recorded <c>OnError</c> notification.</summary>
    OnError,

    /// <summary>A recorded <c>OnCompleted</c> notification.</summary>
    OnCompleted,
}
