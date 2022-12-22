using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;

namespace Open.IO.Extensions;

public static class PipeReaderExtensions
{
#if NET7_0_OR_GREATER
	const int DEFAULT_BUFFER_SIZE = 1024;

	public static async IAsyncEnumerable<ReadOnlyMemory<char>> ReadLinesAsync(
		this PipeReader reader,
		Encoding? encoding = null,
		[EnumeratorCancellation] CancellationToken cancellationToken = default)
	{
		encoding ??= Encoding.UTF8;
		ReadOnlyMemory<byte> newLine = encoding.GetBytes(Environment.NewLine).AsMemory();

		var holdover = new ArrayBufferWriter<byte>(DEFAULT_BUFFER_SIZE);
		var lineWriter = new ArrayBufferWriter<char>(DEFAULT_BUFFER_SIZE);

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

	internal static void Write<T>(this IBufferWriter<T> writer, ReadOnlySequence<T> sequence)
	{
		foreach (var mem in sequence)
			writer.Write(mem.Span);
	}
#endif

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
	/// Yields each memory block contained by a sequence of sequences.
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