import ArgumentParser
import Atomics
import NIOCore
import NIOConcurrencyHelpers
import NIOPosix
import NIOHTTP1
import NIOHTTP2
import Histogram
#if canImport(Network)
import NIOTransportServices
#endif

@main
struct App: AsyncParsableCommand {
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

    @Argument
    var url: String

    @Argument(help: "Saves a HdrHistogram (.hgrm) file to the specified location. This file can be inspected using a HdrHistogram Plotter.", completion: .directory)
    var out: String?

    enum CodingKeys: CodingKey {
        case threads, concurrency, throughput, timeout, duration, connectionReuse, url, out
    }

    // TODO: http2 support

    typealias Clock = ContinuousClock

    private let clock = ContinuousClock()
    private let total = ManagedAtomic(Int64(0))
    private let errors = ManagedAtomic(Int64(0))
    private let histogram = NIOLockedValueBox(Histogram<UInt64>(highestTrackableValue: 60_000_000)) // max: 1 min in usec

    private var eventLoopGroup: (any EventLoopGroup)!
    private var _bootstrap: (any ConnectionTargetBootstrap)!
    private var start: Clock.Instant!
    private var end: Clock.Instant!

    mutating func run() async throws {
        let (_ /* scheme */, target, uri) = try deconstructURL(url) // TODO: handle tls

        eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: threads)
        _bootstrap = makeBootstrap()

        print("Running \(duration)s benchmark @ \(url)")
        print("  \(threads) threads and \(concurrency) connections")
        start = clock.now
        end = start.advanced(by: .seconds(duration))
        let head = HTTPRequestHead(version: .http1_1, method: .GET, uri: uri)
        while end > .now {
            if throughput != 0 {
                await runConstantThroughput(target: target, head: head)
            } else {
                await withDiscardingTaskGroup { group in
                    runConstantConcurrency(target: target, head: head, group: &group)
                }
            }

        }

        try await eventLoopGroup.shutdownGracefully()

        try await printResults()
    }

    func runConstantConcurrency(target: ConnectionTarget, head: HTTPRequestHead, group: inout DiscardingTaskGroup) {
        for _ in 0..<concurrency { // burst
            group.addTask {
                do {
                    let initialStart = clock.now
                    var initialDone = false
                    let clientChannel = try await makeHTTP1Channel(target: target)

                    try await clientChannel.executeThenClose { inbound, outbound in
                        var iterator = inbound.makeAsyncIterator()
                        let reuse = connectionReuse?.randomElement()
                        var executed = 0
                        while reuse != nil ? executed <= reuse! && end > .now : end > .now {
                            let start = initialDone ? clock.now : initialStart
                            try await executeRequest(start: start, head: head, outbound: outbound, inboundIterator: &iterator)
                            initialDone = true
                            executed += 1
                        }
                    }
                } catch {
                    print(error)
                    errors.wrappingIncrement(ordering: .relaxed)
                }
            }
        }
    }

    func runConstantThroughput(target: ConnectionTarget, head: HTTPRequestHead) async {
        let throughputPerConnection = throughput / concurrency

        @Sendable func tick() async {
            await withDiscardingTaskGroup { connectionGroup in
                for _ in 0..<concurrency {
                    connectionGroup.addTask {
                        var actualThroughput = 0
                        do {
                            func runConnection() async throws {
                                let channel = try await makeHTTP1Channel(target: target)
                                try await channel.executeThenClose { inbound, outbound in
                                    var iterator = inbound.makeAsyncIterator()
                                    for _ in 0..<(connectionReuse?.randomElement() ?? throughputPerConnection) {
                                        if actualThroughput >= throughputPerConnection {
                                            throw CancellationError()
                                        }
                                        do {
                                            try await executeRequest(start: .now, head: head, outbound: outbound, inboundIterator: &iterator)
                                            actualThroughput += 1
                                        } catch is CancellationError {
                                            throw CancellationError()
                                        } catch {
                                            print(error)
                                            errors.wrappingIncrement(ordering: .relaxed)
                                        }
                                    }
                                }
                            }

                            while actualThroughput < throughputPerConnection {
                                try await runConnection()
                            }
                        } catch is CancellationError {
                            return
                        } catch {
                            print(error)
                            errors.wrappingIncrement(ordering: .relaxed)
                        }
                    }
                }
            }
        }

        await withDiscardingTaskGroup { group in
            group.addTask {
                await tick()
            }

            for await _ /* tick */ in AsyncTimerSequence(interval: .seconds(1), clock: clock) {
                if clock.now > end {
                    group.cancelAll()
                    return
                }

                group.addTask { await tick() }
            }
        }
    }

    func printResults() async throws {
        let totalDuration = start.duration(to: .now)
        let totalCount = total.load(ordering: .relaxed)
        print("\nBenchmark results:")
        print("Total duration: \(totalDuration) (target duration: \(duration)s)")
        print("Total requests: \(totalCount)")
        print("Errors: \(errors.load(ordering: .relaxed))")
        if throughput != 0 {
            print("Throughput: \(Double(totalCount) / Double(totalDuration.components.seconds)) (target throughput: \(throughput))")
        } else {
            print("Throughput: \(Double(totalCount) / Double(totalDuration.components.seconds))")
        }
        print("Percentile  | Count     | Value")
        print("------------+-----------+-------------")
        let percentiles = [ 0.0, 50.0, 80.0, 95.0, 99.0, 99.9, 99.99, 99.999, 100.0 ]
        let finalHistogram = histogram.withLockedValue { $0 }
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

    func executeRequest(
        start: Clock.Instant,
        head: HTTPRequestHead,
        outbound: NIOAsyncChannelOutboundWriter<HTTPPart<HTTPRequestHead, ByteBuffer>>,
        inboundIterator: inout NIOAsyncChannelInboundStream<HTTPClientResponsePart>.AsyncIterator
    ) async throws {
        try await outbound.write(.head(head))
        try await outbound.write(.end(nil))
        while let packet = try await inboundIterator.next() {
            switch packet {
            case .head(let head):
                if head.status.code / 100 != 2 {
                    throw HTTPResponseError()
                }
            case .body: continue
            case .end:
                let duration = start.duration(to: clock.now)
                let durationInMicro = UInt64(duration.components.attoseconds / 1_000_000_000_000) + UInt64(duration.components.seconds) * 1_000_000
                histogram.withLockedValue { _ = $0.record(durationInMicro) }
                total.wrappingIncrement(ordering: .relaxed)
                return
            }
        }
    }
}


// MARK: Channels

extension App {

    func makeHTTP1Channel(target: ConnectionTarget) async throws -> NIOAsyncChannel<HTTPClientResponsePart, HTTPPart<HTTPRequestHead, ByteBuffer>> {
        try await _bootstrap.connect(target: target) { channel in
                channel.eventLoop.makeCompletedFuture {
                    try channel.pipeline.syncOperations.addHTTPClientHandlers()
                    try channel.pipeline.syncOperations.addHandler(HTTPByteBufferResponsePartHandler())
                    return try NIOAsyncChannel(wrappingChannelSynchronously: channel)
                }
            }
    }

    func makeBootstrap() -> any ConnectionTargetBootstrap {
        #if canImport(Network)
        if let tsBootstrap = NIOTSConnectionBootstrap(validatingGroup: eventLoopGroup) {
            return tsBootstrap
                .connectTimeout(.seconds(timeout))
                .channelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
                .channelOption(ChannelOptions.tcpOption(.tcp_nodelay), value: 1)
        }
        #endif

        guard let bootstrap = ClientBootstrap(validatingGroup: eventLoopGroup) else {
            fatalError("No matching bootstrap found")
        }

        return bootstrap
            .connectTimeout(.seconds(timeout))
            .channelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .channelOption(ChannelOptions.tcpOption(.tcp_nodelay), value: 1)
    }
}


// MARK: Utility

extension App {
    final class HTTPByteBufferResponsePartHandler: ChannelOutboundHandler {
        typealias OutboundIn = HTTPPart<HTTPRequestHead, ByteBuffer>
        typealias OutboundOut = HTTPClientRequestPart

        func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
            let part = self.unwrapOutboundIn(data)
            switch part {
            case .head(let head):
                context.write(self.wrapOutboundOut(.head(head)), promise: promise)
            case .body(let buffer):
                context.write(self.wrapOutboundOut(.body(.byteBuffer(buffer))), promise: promise)
            case .end(let trailers):
                context.write(self.wrapOutboundOut(.end(trailers)), promise: promise)
            }
        }
    }

    struct HTTPResponseError: Error { }
}
