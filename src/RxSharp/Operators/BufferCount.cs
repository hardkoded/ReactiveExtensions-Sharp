namespace RxSharp.Operators;

public static class BufferCountOperator
{
    public static Observable<IReadOnlyList<T>> BufferCount<T>(this Observable<T> source, int bufferSize, int? startBufferEvery = null)
    {
        var every = startBufferEvery ?? bufferSize;

        return source.Operate<T, IReadOnlyList<T>>((src, subscriber) =>
        {
            var buffers = new List<List<T>>();
            var count = 0;

            return src.Subscribe(
                onNext: value =>
                {
                    List<List<T>>? toEmit = null;

                    if (count++ % every == 0)
                    {
                        buffers.Add(new List<T>());
                    }

                    foreach (var buffer in buffers)
                    {
                        buffer.Add(value);
                        if (bufferSize <= buffer.Count)
                        {
                            (toEmit ??= new List<List<T>>()).Add(buffer);
                        }
                    }

                    if (toEmit is not null)
                    {
                        foreach (var buffer in toEmit)
                        {
                            buffers.Remove(buffer);
                            subscriber.OnNext(buffer);
                        }
                    }
                },
                onError: subscriber.OnError,
                onComplete: () =>
                {
                    foreach (var buffer in buffers)
                    {
                        subscriber.OnNext(buffer);
                    }

                    subscriber.OnCompleted();
                });
        });
    }
}
