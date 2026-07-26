using RxSharp.Extras;

namespace RxSharp.Tests.Samples;

/// <summary>
/// Not an NUnit test: a real, compiled source for the README's "retry until timeout" example — the exact
/// combinator behind Puppeteer's own <c>Locator.click()</c>/<c>.fill()</c> actions, proven end-to-end against a
/// real launched Chrome in this project's M3 playground (see CLAUDE.md). Kept in sync with the README via
/// MarkdownSnippets, same as <see cref="QuickTasteSample"/>.
/// </summary>
public static class RetryUntilTimeoutSample
{
    // begin-snippet: retry-until-timeout-csharp
    // Retries a flaky async operation - "find an element that may not have rendered yet" is the
    // Puppeteer case, but this works for any operation that fails until some condition is met -
    // until it succeeds, times out, or the caller cancels.
    public static async Task<string> FindElementOnceItRendersAsync(Func<Task<string>> tryFindElement, CancellationToken cancellationToken)
        => await Observable.Defer(() => Observable.From(tryFindElement()))
            .RetryAndRaceWithSignalAndTimer(TimeSpan.FromSeconds(5), cancellationToken)
            .FirstValueFrom().ConfigureAwait(false);
    // end-snippet
}
