using BenchmarkDotNet.Attributes;
using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.IO.Pipelines;
using System.Text;

namespace Open.IO.Extensions.Tests;

[MemoryDiagnoser]
public class TextReaderExtensionsTests : IDisposable
{
	private static readonly string TestData = File.OpenText("TestData.csv").ReadToEnd();

	private Stream _stream;
	private readonly StringBuilder _sb;

	public TextReaderExtensionsTests()
	{
		_stream = GetStream();
		_sb = new StringBuilder();
	}

	[Params(true, false)]
	public bool UseFile { get; set; }

	[IterationSetup]
	[SuppressMessage("Usage", "xUnit1013:Public method should be marked as test")]
	public void Setup() => _stream = GetStream(UseFile);

	[IterationCleanup]
	[SuppressMessage("Usage", "xUnit1013:Public method should be marked as test")]
	public void Cleanup()
	{
		_stream.Dispose();
		_sb.Clear();
	}

	[SuppressMessage("Usage", "CA1816:Dispose methods should call SuppressFinalize")]
	public void Dispose() => Cleanup();

	private static Stream GetStream(bool useFile = true)
	{
		if (useFile)
			return File.OpenRead("TestData.csv");

		// Create a new MemoryStream object
		MemoryStream stream = new();

		// Convert the string to a byte array
		byte[] bytes = Encoding.UTF8.GetBytes(TestData);

		// Write the bytes to the stream
		stream.Write(bytes, 0, bytes.Length);

		// Reset the position of the stream to the beginning
		stream.Position = 0;
		return stream;
	}

	[Fact, Benchmark(Baseline = true)]
	public async Task SingleBufferReadAsync()
	{
		using var reader = new StreamReader(_stream);
		await foreach (var e in reader.SingleBufferReadAsync())
		{
			_sb.Append(e.Span);
		}
		Assert.Equal(TestData, _sb.ToString());
	}

	[Fact, Benchmark]
	public async Task DualBufferReadAsync()
	{
		using var reader = new StreamReader(_stream);
		await foreach (var e in reader.DualBufferReadAsync())
		{
			_sb.Append(e.Span);
		}
		Assert.Equal(TestData, _sb.ToString());
	}

	[Fact, Benchmark]
	public async Task PreemptiveReadLinesAsync()
	{
		using var reader = new StreamReader(_stream);
		await foreach (var e in reader.PreemptiveReadLinesAsync())
		{
			_sb.AppendLine(e); // this won't always work as the last line may not end in a line ending.
		}
		Assert.Equal(TestData, _sb.ToString());
	}

	[Fact, Benchmark]
	public async Task PipeReaderReadLinesAsync()
	{
		var pool = MemoryPool<byte>.Shared;
		await foreach (var line in PipeReader
			.Create(_stream, new StreamPipeReaderOptions(MemoryPool<byte>.Shared, bufferSize: 1024))
			.ReadLinesAsync())
		{
			_sb.Append(line.Span).AppendLine();
		}

		Assert.Equal(TestData, _sb.ToString());
	}
}