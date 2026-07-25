namespace RxSharp.Tests;

// Ported (non-marble subset) from rxjs 7.8.2 spec/firstValueFrom-spec.ts and spec/lastValueFrom-spec.ts.
[TestFixture]
public class AsyncBridgeTests
{
    [Test]
    public async Task FirstValueFrom_ShouldResolveWithTheFirstValue()
    {
        var value = await Observable.Of(1, 2, 3).FirstValueFrom().ConfigureAwait(false);

        Assert.That(value, Is.EqualTo(1));
    }

    [Test]
    public void FirstValueFrom_ShouldRejectWithEmptyErrorWhenSourceCompletesWithoutEmitting()
    {
        Assert.ThrowsAsync<EmptyErrorException>(() => Observable.Empty<int>().FirstValueFrom());
    }

    [Test]
    public async Task FirstValueFrom_ShouldResolveWithTheDefaultValueWhenSourceIsEmpty()
    {
        var value = await Observable.Empty<int>().FirstValueFrom(42).ConfigureAwait(false);

        Assert.That(value, Is.EqualTo(42));
    }

    [Test]
    public void FirstValueFrom_ShouldRejectWhenSourceErrors()
    {
        var error = new InvalidOperationException("boom");
        var ex = Assert.ThrowsAsync<InvalidOperationException>(() => Observable.ThrowError<int>(() => error).FirstValueFrom());

        Assert.That(ex, Is.SameAs(error));
    }

    [Test]
    public async Task LastValueFrom_ShouldResolveWithTheLastValue()
    {
        var value = await Observable.Of(1, 2, 3).LastValueFrom().ConfigureAwait(false);

        Assert.That(value, Is.EqualTo(3));
    }

    [Test]
    public void LastValueFrom_ShouldRejectWithEmptyErrorWhenSourceCompletesWithoutEmitting()
    {
        Assert.ThrowsAsync<EmptyErrorException>(() => Observable.Empty<int>().LastValueFrom());
    }

    [Test]
    public async Task LastValueFrom_ShouldResolveWithTheDefaultValueWhenSourceIsEmpty()
    {
        var value = await Observable.Empty<int>().LastValueFrom(42).ConfigureAwait(false);

        Assert.That(value, Is.EqualTo(42));
    }
}
