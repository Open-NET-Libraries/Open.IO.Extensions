using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Pipelines;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Open.IO.Extensions;

public static class AsyncEnumerableExtensions
{
	const int DEFAULT_BUFFER_SIZE = 1024;

	/// <summary>
	/// Reads available bytes in a stream into an in-memory buffer and yields the portion of the buffer that was filled.
	/// </summary>
	/// <param name="stream">The stream to buffer.</param>
	/// <param name="bufferSize">The minimum size of the in-memory buffer.</param>
	/// <param name="cancellationToken">An optional cancellation token.</param>
	/// <returns>An <see langword="async"/> enumerable that yields an in-memory buffer for each block read from the stream.</returns>
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

		(cCurrent, cNext) = (cNext, cCurrent);
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

		(cCurrent, cNext) = (cNext, cCurrent);
		next = current;

		goto loop;
	}

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

#if NET7_0_OR_GREATER
	public static async IAsyncEnumerable<ReadOnlyMemory<char>> ReadLinesAsync(
		this PipeReader reader,
		Encoding? encoding = null,
		[EnumeratorCancellation] CancellationToken cancellationToken = default)
	{
		encoding ??= Encoding.UTF8;
		ReadOnlyMemory<byte> newLine = encoding.GetBytes(Environment.NewLine).AsMemory();

		var holdover = new ArrayBufferWriter<byte>();
		var lineWriter = new ArrayBufferWriter<char>();

		long bufferSize = 0;
		while (true)
		{
			// Get some data.
			var result = await reader.ReadAsync(cancellationToken);
			if (result.IsCompleted || result.IsCanceled)
				break;

			var buffer = result.Buffer;
			if (buffer.IsEmpty)
				break;

			// Buffer didn't grow?
			if (buffer.Length == bufferSize)
			{
				// We are at the end of a sequence, but another might be coming so we have to capture it and cleanup.
				holdover.Write(buffer);
				reader.AdvanceTo(buffer.End);
				bufferSize = 0;
				continue;
			}

			SequencePosition? newPos = null;
			while (true)
			{
				// See if the data is enough to be a line.
				var pos = TryGetNextLine(in buffer, in newLine, out var line);
				if (!pos.HasValue) break;

				if (holdover.WrittenCount == 0)
				{
					encoding.GetChars(line, lineWriter);
				}
				else
				{
					holdover.Write(line);
					encoding.GetChars(holdover.WrittenSpan, lineWriter);
					holdover.Clear();
				}

				yield return lineWriter.WrittenMemory;
				lineWriter.Clear();

				var p = pos.Value;
				buffer = buffer.Slice(p);
				newPos = p;
			}

			if (newPos.HasValue)
				reader.AdvanceTo(newPos.Value);

			bufferSize = buffer.Length;
		}

		// Last remaining bytes?
		if (holdover.WrittenCount != 0)
		{
			encoding.GetChars(holdover.WrittenSpan, lineWriter);
			holdover.Clear();
			yield return lineWriter.WrittenMemory;
			lineWriter.Clear();
		}

		await reader.CompleteAsync();
	}
#endif

	static SequencePosition? TryGetNextLine(
		in ReadOnlySequence<byte> buffer,
		in ReadOnlyMemory<byte> search,
		out ReadOnlySequence<byte> line)
	{
		var sr = new SequenceReader<byte>(buffer);
		if (sr.End) goto none;
		if (sr.TryReadTo(out line, search.Span, true))
			return sr.Position;

		none:
		line = default;
		return default;
	}

	internal static void Write<T>(this ArrayBufferWriter<T> writer, ReadOnlySequence<T> sequence)
	{
		foreach (var mem in sequence)
			writer.Write(mem.Span);
	}

	internal static IMemoryOwner<char> GetChars(this Encoding encoding, in ReadOnlyMemory<byte> bytes, MemoryPool<char>? pool = null)
	{
		pool ??= MemoryPool<char>.Shared;
		var charCount = encoding.GetCharCount(bytes.Span);
		var owner = pool.Rent(charCount);
		encoding.GetChars(bytes.Span, owner.Memory.Span);
		return owner;
	}


	/** The following method is based upon:
	 ** https://github.com/ByteTerrace/ByteTerrace.Ouroboros.Core/blob/702facd67bf9e9840ac8e1fe9c40dceec434f262/PipeReaderExtensions.cs#L9-L31
	 ** Thank you to the author for the reference. **/

	/// <summary>
	/// Yields the buffer of a PipeReader for every block read by reader.ReadAsync().
	/// </summary>
	/// <param name="reader">The PipeReader to read from.</param>
	/// <param name="cancellationToken">An optional cancellation token.</param>
	/// <returns>An <see langword="async"/> enumerable that yields the buffer of the PipeReader for each block read from the reader.</returns>
	public static async IAsyncEnumerable<ReadOnlySequence<byte>> EnumerateAsync(
		this PipeReader reader,
		[EnumeratorCancellation] CancellationToken cancellationToken = default)
	{
	start:
		var readResult = await reader
			.ReadAsync(cancellationToken: cancellationToken)
			.ConfigureAwait(continueOnCapturedContext: false);

		if (readResult.IsCompleted)
		{
			await reader.CompleteAsync();
			yield break;
		}

		try
		{
			yield return readResult.Buffer;
		}
		finally
		{
			reader.AdvanceTo(consumed: readResult.Buffer.End);
		}

		goto start;
	}

	/// <summary>
	/// Partitions each memory block contained by a sequence.
	/// </summary>
	/// <param name="sequences">The sequences to partition.</param>
	/// <param name="cancellationToken">An optional cancellation token.</param>
	/// <returns>An <see langword="async"/> enumerable that yields each block of memory of each of the provided sequences.</returns>
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