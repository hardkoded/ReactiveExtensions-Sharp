namespace RxSharp.Operators;

/// <summary>Implements the <c>BufferCount</c> operator. Mirrors rxjs's <c>bufferCount</c>.</summary>
public static class BufferCountOperator
{
    /// <summary>
    /// Collects values from <paramref name="source"/> into a buffer and emits the buffer, as a read-only list,
    /// once it reaches <paramref name="bufferSize"/>.
    /// </summary>
    /// <remarks>
    /// <paramref name="startBufferEvery"/> controls how often a new buffer is started (every Nth value, counting
    /// from the first). When omitted, it defaults to <paramref name="bufferSize"/>, so buffers are opened right
    /// after the previous one closes, producing non-overlapping chunks of <paramref name="bufferSize"/> values.
    /// A smaller value makes buffers overlap (multiple buffers open at once); a larger value skips values
    /// between buffers. If <paramref name="source"/> completes while one or more buffers are still short of
    /// <paramref name="bufferSize"/>, those partial buffers are emitted as-is, in the order they were opened,
    /// before completion is forwarded.
    /// </remarks>
    /// <typeparam name="T">The type of the elements collected into each buffer.</typeparam>
    /// <param name="source">The source observable to buffer.</param>
    /// <param name="bufferSize">The number of values a buffer must contain before it is emitted.</param>
    /// <param name="startBufferEvery">
    /// The number of values between the start of one buffer and the next. Defaults to <paramref name="bufferSize"/>
    /// (non-overlapping buffers) when <see langword="null"/>.
    /// </param>
    /// <returns>An observable of each completed (or trailing partial) buffer as an <see cref="IReadOnlyList{T}"/>.</returns>
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
