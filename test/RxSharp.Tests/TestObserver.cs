namespace RxSharp.Tests;

/// <summary>Collects emissions from a subscription for assertions. The NUnit-side equivalent of hand-rolled observers used throughout the rxjs spec suite.</summary>
internal sealed class TestObserver<T> : IObserver<T>
{
    public List<T> Values { get; } = [];

    public List<Exception> Errors { get; } = [];

    public int CompletedCount { get; private set; }

    public void OnNext(T value) => Values.Add(value);

    public void OnError(Exception error) => Errors.Add(error);

    public void OnCompleted() => CompletedCount++;
}
