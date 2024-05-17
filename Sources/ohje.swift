import ArgumentParser
import Atomics
import NIOCore
import NIOConcurrencyHelpers
import NIOPosix
import NIOHTTP1
import NIOHTTP2
import Histogram

@main
struct App: AsyncParsableCommand {
    @Option(name: .shortAndLong, help: "Number of cpu threads to be used.")
    var threads: Int = 1

    @Option(name: .shortAndLong, help: "Number of concurrent connections to keep open.")
    var concurrency: Int = 10

    @Option(name: .long, help: "The desired throughput per second.")
    var throughput: Int = 10000

    @Option(name: .long, help: "The tcp timeout before a connection attempt gets aborted.")
    var timeout: Int64 = 30

    @Option(name: .short, help: "The duration of the benchmark in seconds.")
    var duration: Int = 10

    @Argument
    var url: String

    @Argument(help: "Saves a HdrHistogram (.hgrm) file to the specified location. This file can be inspected using a HdrHistogram Plotter.")
    var out: String?

    // TODO: http2 support

    func run() async throws {
        let (scheme, target, uri) = try deconstructURL(url) // TODO: handle scheme

        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: threads)

        let clock = ContinuousClock()
        let total = ManagedAtomic(Int64(0))
        let errors = ManagedAtomic(Int64(0))
        let histogram = NIOLockedValueBox(Histogram<UInt64>(highestTrackableValue: 60_000_000)) // max: 1 min in usec

        print("running benchmark for \(duration)s...")
        let start = clock.now
        let end = start.advanced(by: .seconds(duration))
        while end > .now {
            await withDiscardingTaskGroup { group in
                for _ in 0..<concurrency { // burst
                    group.addTask {
                        do {
                            let initialStart = clock.now // TODO: do we measure the tcp connection + request or just the http stuff?
                            var initialDone = false
                            let clientChannel = try await ClientBootstrap(group: eventLoopGroup)
                                .connectTimeout(.seconds(timeout))
                                .connect(target: target) { channel in
                                    channel.eventLoop.makeCompletedFuture {
                                        try channel.pipeline.syncOperations.addHTTPClientHandlers()
                                        try channel.pipeline.syncOperations.addHandler(HTTPByteBufferResponsePartHandler())
                                        return try NIOAsyncChannel<HTTPPart<HTTPResponseHead, ByteBuffer>, HTTPPart<HTTPRequestHead, ByteBuffer>>(wrappingChannelSynchronously: channel)
                                    }
                                }

                            try await clientChannel.executeThenClose { inbound, outbound in
                                var iterator = inbound.makeAsyncIterator()
                                let head = HTTPRequestHead(version: .http1_1, method: .GET, uri: uri)
                                loop: while end > .now {
                                    let start = initialDone ? clock.now : initialStart
                                    try await outbound.write(.head(head))
                                    try await outbound.write(.end(nil))
                                    while let packet = try await iterator.next() {
                                        switch packet {
                                        case .head(let head):
                                            if head.status.code / 100 != 2 {
                                                throw HTTPResponseError()
                                            }
                                        case .body: continue
                                        case .end:
                                            initialDone = true
                                            let duration = start.duration(to: clock.now)
                                            let durationInMicro = UInt64(duration.components.attoseconds / 1000000000000) + UInt64(duration.components.seconds) * 1000000
                                            histogram.withLockedValue { _ = $0.record(durationInMicro) }
                                            total.wrappingIncrement(ordering: .relaxed)
                                            continue loop
                                        }
                                    }
                                }
                            }
                        } catch {
                            errors.wrappingIncrement(ordering: .relaxed)
                        }
                    }
                }
            }

        }

        try await eventLoopGroup.shutdownGracefully()

        let totalDuration = start.duration(to: .now)
        let totalCount = total.load(ordering: .relaxed)
        print("benchmark results:")
        print("total duration: \(totalDuration) (target duration: \(duration)s)")
        print("total requests: \(totalCount)")
        print("errors: \(errors.load(ordering: .relaxed))")
        print("throughput: \(Double(totalCount) / Double(totalDuration.components.seconds)) (target throughput: \(throughput))")
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

struct OutputBuffer: TextOutputStream {
    private var underlying = ByteBuffer()

    mutating func write(_ string: String) {
        underlying.writeString(string)
    }

    func persist(to file: String) async throws {
        let fileIO = NonBlockingFileIO(threadPool: .singleton)
        try await fileIO.withFileHandle(path: file, mode: .write, flags: .allowFileCreation()) { handle in
            try await fileIO.write(fileHandle: handle, buffer: underlying)
        }
    }
}
