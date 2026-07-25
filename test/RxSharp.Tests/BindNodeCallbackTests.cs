namespace RxSharp.Tests;

/// <summary>
/// Ported from rxjs's <c>bindNodeCallback-spec.ts</c> (tag 7.8.2). The "when scheduled" describe block and the
/// JS-`this`-context tests have no C# equivalent -- see <see cref="Observable"/>'s <c>BindNodeCallback</c> XML
/// docs for the documented deviations (no scheduler parameter, no resultSelector parameter, single-result-value
/// callbacks only, error mapped to <see cref="Exception"/>? instead of JS's "null means no error" convention).
/// </summary>
[TestFixture]
public class BindNodeCallbackTests
{
    [Test]
    public void ShouldEmitUnitWhenCallbackIsCalledWithoutSuccessArguments()
    {
        void Callback(Action<Exception?> cb) => cb(null);

        var boundCallback = Observable.BindNodeCallback(Callback);
        var results = new List<object>();

        boundCallback().Subscribe(x => results.Add(x), onComplete: () => results.Add("done"));

        Assert.That(results, Is.EqualTo(new object[] { Unit.Default, "done" }));
    }

    [Test]
    public void ShouldEmitOneValueFromACallback()
    {
        void Callback(int datum, Action<Exception?, int> cb) => cb(null, datum);

        var boundCallback = Observable.BindNodeCallback<int, int>(Callback);
        var results = new List<object>();

        boundCallback(42).Subscribe(x => results.Add(x), onComplete: () => results.Add("done"));

        Assert.That(results, Is.EqualTo(new object[] { 42, "done" }));
    }

    [Test]
    public void ShouldRaiseErrorFromCallback()
    {
        var error = new InvalidOperationException("boom");
        void Callback(Action<Exception?> cb) => cb(error);

        var boundCallback = Observable.BindNodeCallback(Callback);
        var results = new List<Exception>();

        boundCallback().Subscribe(
            _ => Assert.Fail("should not next"),
            err => results.Add(err),
            () => Assert.Fail("should not complete"));

        Assert.That(results, Is.EqualTo(new[] { error }));
    }

    [Test]
    public void ShouldNotEmitThrowOrCompleteIfImmediatelyUnsubscribed()
    {
        Action? fireCallback = null;
        void Callback(int datum, Action<Exception?, int> cb) => fireCallback = () => cb(null, datum);

        var boundCallback = Observable.BindNodeCallback<int, int>(Callback);
        var next = false;
        var error = false;
        var complete = false;

        var subscription = boundCallback(42).Subscribe(_ => next = true, _ => error = true, () => complete = true);
        subscription.Dispose();

        fireCallback!();

        Assert.That(next, Is.False);
        Assert.That(error, Is.False);
        Assert.That(complete, Is.False);
    }

    [Test]
    public void ShouldCreateASeparateInternalSubjectForEachCall()
    {
        void Callback(int datum, Action<Exception?, int> cb) => cb(null, datum);

        var boundCallback = Observable.BindNodeCallback<int, int>(Callback);
        var results = new List<object>();

        boundCallback(42).Subscribe(x => results.Add(x), onComplete: () => results.Add("done"));
        boundCallback(54).Subscribe(x => results.Add(x), onComplete: () => results.Add("done"));

        Assert.That(results, Is.EqualTo(new object[] { 42, "done", 54, "done" }));
    }

    [Test]
    public void ShouldEmitPostCallbackErrors()
    {
        void BadFunction(Action<Exception?, int> callback)
        {
            callback(null, 42);
            throw new InvalidOperationException("kaboom");
        }

        var boundCallback = Observable.BindNodeCallback<int>(BadFunction);
        Exception? received = null;

        boundCallback().Subscribe(onError: err => received = err);

        Assert.That(received, Is.Not.Null);
        Assert.That(received!.Message, Is.EqualTo("kaboom"));
    }

    [Test]
    public void ShouldCacheValueForNextSubscriptionAndNotCallCallbackFuncAgain()
    {
        var calls = 0;
        void Callback(int datum, Action<Exception?, int> cb)
        {
            calls++;
            cb(null, datum);
        }

        var boundCallback = Observable.BindNodeCallback<int, int>(Callback);
        var results1 = new List<object>();
        var results2 = new List<object>();

        var source = boundCallback(42);

        source.Subscribe(x => results1.Add(x), onComplete: () => results1.Add("done"));
        source.Subscribe(x => results2.Add(x), onComplete: () => results2.Add("done"));

        Assert.That(calls, Is.EqualTo(1));
        Assert.That(results1, Is.EqualTo(new object[] { 42, "done" }));
        Assert.That(results2, Is.EqualTo(new object[] { 42, "done" }));
    }

    [Test]
    public void ShouldNotCallTheFunctionIfSubscribedTwiceInARowBeforeItResolves()
    {
        Action<Exception?, int>? executeCallback = null;
        var calls = 0;
        void MyFunc(Action<Exception?, int> callback)
        {
            calls++;
            if (calls > 1)
            {
                throw new InvalidOperationException("too many calls to myFunc");
            }

            executeCallback = callback;
        }

        var source = Observable.BindNodeCallback<int>(MyFunc)();

        int? result1 = null;
        int? result2 = null;
        source.Subscribe(value => result1 = value);
        source.Subscribe(value => result2 = value);

        Assert.That(calls, Is.EqualTo(1));

        executeCallback!(null, 99);

        Assert.That(result1, Is.EqualTo(99));
        Assert.That(result2, Is.EqualTo(99));
        Assert.That(calls, Is.EqualTo(1));
    }

    [Test]
    public void ShouldSupportTwoInputArguments()
    {
        void Callback(int a, int b, Action<Exception?, int> cb) => cb(null, a + b);

        var boundCallback = Observable.BindNodeCallback<int, int, int>(Callback);
        int? result = null;

        boundCallback(2, 3).Subscribe(x => result = x);

        Assert.That(result, Is.EqualTo(5));
    }

    [Test]
    public void ShouldSupportThreeInputArguments()
    {
        void Callback(int a, int b, int c, Action<Exception?, int> cb) => cb(null, a + b + c);

        var boundCallback = Observable.BindNodeCallback<int, int, int, int>(Callback);
        int? result = null;

        boundCallback(2, 3, 4).Subscribe(x => result = x);

        Assert.That(result, Is.EqualTo(9));
    }
}
