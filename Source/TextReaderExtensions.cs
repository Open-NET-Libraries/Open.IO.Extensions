using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Open.IO.Extensions;

public static class TextReaderExtensions
{
	const int DEFAULT_BUFFER_SIZE = 1024;

	/// <summary>
	/// Reads available characters in a TextReader into an in-memory buffer and yields the portion of the buffer that was filled.
	/// </summary>
	/// <param name="reader">The TextReader to buffer.</param>
	/// <param name="bufferSize">The minimum size of the in-memory buffer.</param>
	/// <param name="cancellationToken">An optional cancellation token.</param>
	/// <returns>An <see langword="async"/> enumerable that yields an in-memory buffer for each block read from the reader.</returns>
	public static async IAsyncEnumerable<ReadOnlyMemory<char>> SingleBufferReadAsync(
		this TextReader reader,
		int bufferSize = DEFAULT_BUFFER_SIZE,
		[EnumeratorCancellation] CancellationToken cancellationToken = default)
	{
		var pool = MemoryPool<char>.Shared;
		using var A = pool.Rent(bufferSize);
		var buffer = A.Memory;

	loop:
		var n = await reader.ReadAsync(buffer, cancellationToken).ConfigureAwait(false);
		if (n == 0) yield break;
		yield return n == buffer.Length ? buffer : buffer.Slice(0, n);

		goto loop;
	}

	/// <summary>
	/// Reads available bytes in a stream into two in-memory buffers and yields (in order) the portion of each buffer that was filled.
	/// </summary>
	/// <remarks>Theoretically the dual buffer approach can improve throughput when there is downstream processing.</remarks>
	/// <inheritdoc cref="SingleBufferReadAsync(TextReader, int, CancellationToken)"/>
	public static async IAsyncEnumerable<ReadOnlyMemory<char>> DualBufferReadAsync(
		this TextReader reader,
		int bufferSize = DEFAULT_BUFFER_SIZE,
		[EnumeratorCancellation] CancellationToken cancellationToken = default)
	{
		var pool = MemoryPool<char>.Shared;
		using var A = pool.Rent(bufferSize);
		using var B = pool.Rent(bufferSize);
		var cNext = A.Memory;
		var cCurrent = B.Memory;

		var next = reader.ReadAsync(cNext, cancellationToken);

	loop:
		var n = await next.ConfigureAwait(false);
		if (n == 0) yield break;

		// Preemptive request before yielding.
		var current = reader.ReadAsync(cCurrent, cancellationToken);
		yield return n == cNext.Length ? cNext : cNext.Slice(0, n);

		(cCurrent, cNext) = (cNext, cCurrent);
		next = current;

		goto loop;
	}

	/// <summary>
	/// Asynchronously reads one line ahead while yielding the current line.
	/// </summary>
	/// <remarks>
	/// Commits a string to memory for every line. Using another method may be preferred.
	/// </remarks>
	public static async IAsyncEnumerable<string> PreemptiveReadLinesAsync(
		this TextReader reader,
		[EnumeratorCancellation] CancellationToken cancellationToken = default)
	{
		var next = Next(reader, cancellationToken);

	loop:
		var n = await next.ConfigureAwait(false);
		if (n is null) yield break;

		// Preemptive request before yielding.
		next = Next(reader, cancellationToken);
		yield return n;

		goto loop;

		static async ValueTask<string?> Next(TextReader reader, CancellationToken token)
		{
			if (token.IsCancellationRequested)
				return null;

#if NET7_0_OR_GREATER
			try
			{
				return await reader.ReadLineAsync(token);
			}
			catch (OperationCanceledException)
			{
				return null;
			}
#else
			return await reader.ReadLineAsync();
#endif
		}
	}
}