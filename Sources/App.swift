import ArgumentParser
import NIOPosix
import Histogram

@main
struct App: AsyncParsableCommand, Arguments {
    @Option(name: .long, help: "Number of cpu threads to be used.")
    var threads: Int = 2

    @Option(name: .shortAndLong, help: "Number of concurrent connections to keep open.")
    var concurrency: Int = 10

    @Option(name: .shortAndLong, help: "The constant throughput per second. If 0, requests are sent back to back without fixed latency.")
    var throughput: Int = 1000

    @Option(name: .long, help: "The tcp timeout before a connection attempt gets aborted.")
    var timeout: Int64 = 30

    @Option(name: .shortAndLong, help: "The duration of the benchmark in seconds.")
    var duration: Int = 10

    @Option(name: .long, help: """
    Indicates how many requests are executed on a single TCP connection before a new connection is created. 
    Can be specified as a range or an exact value. If not specified, connections are reused for [throughput/concurrency] \
    or indefinitely if thoughput is 0.

    Examples:
      - 0..<100 : connections will be reused between 0 and 99 times.
      - 10...50 : connections will be reused between 10 and 50 times.
      - 20      : connections will be reused exactly 20 times.
      - 1...    : connections will be reused at least once.
      - ...20   : connections will be reused up to 20 times.
      - ..<10   : connections will be reused up to 9 times.
    """, transform: parseRange(_:))
    var connectionReuse: Range<Int>?

    @Flag
    var http2: Bool = false

    @Argument
    var url: String

    @Argument(help: "Saves a HdrHistogram (.hgrm) file to the specified location. This file can be inspected using a HdrHistogram Plotter.", completion: .directory)
    var out: String?

    mutating func run() async throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: threads)
        let clock = ContinuousClock()

        print("Running \(duration)s benchmark @ \(url)")
        print("  \(threads) threads and \(concurrency) connections")
        let start = clock.now
        let end = start.advanced(by: .seconds(duration))

        let runner = try BenchmarkRunner(
            self,
            clock: clock,
            start: start,
            end: end,
            on: eventLoopGroup
        )
        let result = try await runner.run()
        try await runner.shutdown()

        try await printResults(result)
    }

    func printResults(_ result: BenchmarkResult) async throws {
        print("\nBenchmark results:")
        print("Total duration: \(result.duration) (target duration: \(duration)s)")
        print("Total requests: \(result.total)")
        print("Errors: \(result.errors)")
        if throughput != 0 {
            print("Throughput: \(result.throughput) (target throughput: \(throughput))")
        } else {
            print("Throughput: \(result.throughput)")
        }
        print("Percentile  | Count     | Value")
        print("------------+-----------+-------------")
        let percentiles = [ 0.0, 50.0, 80.0, 95.0, 99.0, 99.9, 99.99, 99.999, 100.0 ]
        let finalHistogram = result.histogram
        let firstValue = finalHistogram.valueAtPercentile(0.0)
        for p in percentiles {
            let value = finalHistogram.valueAtPercentile(p)
            let count = finalHistogram.count(within: firstValue...value)
            print("\(String(format: "%-08.3f", p))    | \(String(format: "%-09d", count)) | \(Double(value) / 1000000)s")
        }

        if let out {
            var buffer = OutputBuffer()
            finalHistogram.write(to: &buffer)
            try await buffer.persist(to: out)
        }
    }
}
