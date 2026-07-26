using RxSharp.Operators;

namespace RxSharp.Tests.Samples;

/// <summary>
/// Not an NUnit test: a real, compiled source for the README's "Quick taste" example. Kept in sync with the
/// README via MarkdownSnippets (see <c>mdsnippets.json</c> at the repo root and the <c>snippet: quick-taste-csharp</c>
/// marker in <c>README.md</c>) so the pitch example in the README can never silently drift from working code.
/// </summary>
public static class QuickTasteSample
{
    /// <summary>
    /// Builds and subscribes to the same map/filter/takeUntil pipeline shown in the README, using a real
    /// <see cref="Button"/> stand-in instead of a UI framework's own button type.
    /// </summary>
    /// <param name="button">The click source that ends the pipeline.</param>
    public static void Run(Button button)
    {
        // begin-snippet: quick-taste-csharp
        var clicks = Observable.FromEvent<EventArgs>(h => button.Click += h, h => button.Click -= h);

        Observable.Interval(TimeSpan.FromSeconds(1))
            .Map(x => x * x)
            .Filter(x => x % 2 == 0)
            .TakeUntil(clicks)
            .Subscribe(x => Console.WriteLine(x));
        // end-snippet
    }

    /// <summary>A minimal stand-in for a UI button's click event, so <see cref="Run"/> compiles without a real UI framework dependency.</summary>
    public sealed class Button
    {
        /// <summary>Raised when the button is clicked.</summary>
        public event EventHandler<EventArgs>? Click;

        /// <summary>Simulates a user click, raising <see cref="Click"/>.</summary>
        public void RaiseClick() => Click?.Invoke(this, EventArgs.Empty);
    }
}
