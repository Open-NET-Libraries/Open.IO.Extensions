using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Open.IO.Extensions;

public static class AsyncEnumerableExtensions
{
	const int DEFAULT_BUFFER_SIZE = 65536; // A larger default is chosen as it tends to improve the total throughput.

	/// <summary>
	/// Reads available bytes in a stream into an in-memory buffer and yields the portion of the buffer that was filled.
	/// </summary>
	/// <param name="stream">The stream to buffer.</param>
	/// <param name="bufferSize">The minimum size of the in-memory buffer.</param>
	/// <param name="cancellationToken">An optional cancellation token.</param>
	/// <returns>An async enumerable that yields an in-memory buffer for each block read from the stream.</returns>
	public static async IAsyncEnumerable<ReadOnlyMemory<byte>> SingleBufferReadAsync(
		this Stream stream,
		int bufferSize = DEFAULT_BUFFER_SIZE,
		[EnumeratorCancellation] CancellationToken cancellationToken = default)
	{
		var pool = MemoryPool<byte>.Shared;
		using var A = pool.Rent(bufferSize);
		var buffer = A.Memory;

	loop:
		var n = await stream.ReadAsync(buffer, cancellationToken).ConfigureAwait(false);
		if (n == 0) yield break;
		yield return n == buffer.Length ? buffer : buffer.Slice(0, n);
		goto loop;
	}

	/// <summary>
	/// Reads available characters in a TextReader into an in-memory buffer and yields the portion of the buffer that was filled.
	/// </summary>
	/// <param name="reader">The TextReader to buffer.</param>
	/// <param name="bufferSize">The minimum size of the in-memory buffer.</param>
	/// <param name="cancellationToken">An optional cancellation token.</param>
	/// <returns>An async enumerable that yields an in-memory buffer for each block read from the reader.</returns>
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
	/// <inheritdoc cref="SingleBufferReadAsync(Stream, int, CancellationToken)"/>
	public static async IAsyncEnumerable<ReadOnlyMemory<byte>> DualBufferReadAsync(
		this Stream reader,
		int bufferSize = DEFAULT_BUFFER_SIZE,
		[EnumeratorCancellation] CancellationToken cancellationToken = default)
	{
		var pool = MemoryPool<byte>.Shared;
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

		var swap = cNext;
		cNext = cCurrent;
		cCurrent = swap;
		next = current;

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

		var swap = cNext;
		cNext = cCurrent;
		cCurrent = swap;
		next = current;

		goto loop;
	}

	public static async IAsyncEnumerable<string> PreemptiveReadLineAsync(
		this TextReader reader,
		[EnumeratorCancellation] CancellationToken cancellationToken = default)
	{
		var next = reader.ReadLineAsync();

	loop:
		var n = await next.ConfigureAwait(false);
		if (n is null || cancellationToken.IsCancellationRequested)
			yield break;

		// Preemptive request before yielding.
		next = reader.ReadLineAsync();
		yield return n;

		goto loop;
	}

	/** The following method is based upon:
	 ** https://github.com/ByteTerrace/ByteTerrace.Ouroboros.Core/blob/702facd67bf9e9840ac8e1fe9c40dceec434f262/PipeReaderExtensions.cs#L9-L31
	 ** Thank you to the author for the reference. **/

	/// <summary>
	/// Yields the buffer of a PipeReader for every block read by reader.ReadAsync().
	/// </summary>
	/// <param name="reader">The PipeReader to read from.</param>
	/// <param name="cancellationToken">An optional cancellation token.</param>
	/// <returns>An async enumerable that yields the buffer of the PipeReader for each block read from the reader.</returns>
	public static async IAsyncEnumerable<ReadOnlySequence<byte>> EnumerateAsync(
		this PipeReader reader,
		[EnumeratorCancellation] CancellationToken cancellationToken = default)
	{
		var readResult = await reader
			.ReadAsync(cancellationToken: cancellationToken)
			.ConfigureAwait(continueOnCapturedContext: false);

		while (!readResult.IsCompleted)
		{
			try
			{
				yield return readResult.Buffer;
			}
			finally
			{
				reader.AdvanceTo(consumed: readResult.Buffer.End);
			}

			readResult = await reader
				.ReadAsync(cancellationToken: cancellationToken)
				.ConfigureAwait(continueOnCapturedContext: false);
		}
	}

	/// <summary>
	/// Partitions each memory block contained by a sequence.
	/// </summary>
	/// <param name="sequences">The sequences to partition.</param>
	/// <param name="cancellationToken">An optional cancellation token.</param>
	/// <returns>An async enumerable that yields each block of memory of each of the provided sequences.</returns>
	public static async IAsyncEnumerable<ReadOnlyMemory<byte>> AsMemoryAsync(
		this IAsyncEnumerable<ReadOnlySequence<byte>> sequences,
		[EnumeratorCancellation] CancellationToken cancellationToken = default)
	{
		await foreach (var sequence in sequences)
		{
			foreach (var mem in sequence)
				yield return mem;

			if (cancellationToken.IsCancellationRequested)
				break;
		}
	}
}