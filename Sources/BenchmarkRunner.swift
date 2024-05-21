import Atomics
import Histogram
import NIOConcurrencyHelpers
import NIOCore
import NIOPosix
import NIOHTTP1
import NIOSSL
#if canImport(Network)
import NIOTransportServices
#endif

protocol Arguments {
    var concurrency: Int { get }
    var throughput: Int { get }
    var timeout: Int64 { get }
    var connectionReuse: Range<Int>? { get }
    var url: String { get }
}

struct BenchmarkResult {
    let duration: Duration
    let total: Int64
    let errors: Int64
    let throughput: Double
    let histogram: Histogram<UInt64>
}

struct BenchmarkRunner<Clock: _Concurrency.Clock> where Clock.Duration == Duration {
    private let args: Arguments
    private let target: ConnectionTarget
    private let uri: String

    private let total = ManagedAtomic(Int64(0))
    private let errors = ManagedAtomic(Int64(0))
    private let histogram = NIOLockedValueBox(Histogram<UInt64>(highestTrackableValue: 60_000_000)) // max: 1 min in usec

    private let clock: Clock
    private let start: Clock.Instant
    private let end: Clock.Instant
    private let now: @Sendable () -> Clock.Instant

    private let eventLoopGroup: any EventLoopGroup
    private let bootstrap: any ConnectionTargetBootstrap

    private let sslContext: NIOSSLContext?

    init(
        _ args: Arguments,
        clock: Clock,
        start: Clock.Instant,
        end: Clock.Instant,
        now: @escaping @Sendable () -> Clock.Instant,
        on eventLoopGroup: any EventLoopGroup
    ) throws {
        self.args = args
        self.clock = clock
        self.start = start
        self.end = end
        self.now = now
        self.eventLoopGroup = eventLoopGroup

        let (scheme, target, uri) = try deconstructURL(args.url)
        self.target = target
        self.uri = uri

        let sslContext: NIOSSLContext?
        switch scheme {
        case .https, .httpsUnix:
            sslContext = try NIOSSLContext(configuration: .clientDefault) // might be worth adding customisation abilities in the future...
        case .http, .unix, .httpUnix:
            sslContext = nil
        }
        self.sslContext = sslContext

        self.bootstrap = Self.makeBootstrap(timeout: args.timeout, on: eventLoopGroup)
    }

    func shutdown() async throws {
        try await self.eventLoopGroup.shutdownGracefully()
    }

    func run() async throws -> BenchmarkResult {
        let head = HTTPRequestHead(version: .http1_1, method: .GET, uri: uri)
        while end > now() {
            if args.throughput != 0 {
                await runConstantThroughput(head: head)
            } else {
                await withDiscardingTaskGroup { group in
                    runConstantConcurrency(head: head, group: &group)
                }
            }
        }

        let duration = start.duration(to: now())
        let total = total.load(ordering: .relaxed)
        return BenchmarkResult(
            duration: duration,
            total: total,
            errors: errors.load(ordering: .relaxed),
            throughput: Double(total) / Double(duration.components.seconds),
            histogram: histogram.withLockedValue({ $0 })
        )
    }

    private func runConstantConcurrency(head: HTTPRequestHead, group: inout DiscardingTaskGroup) {
        for _ in 0..<args.concurrency { // burst
            group.addTask {
                do {
                    let initialStart = clock.now
                    var initialDone = false
                    let clientChannel = try await makeHTTP1Channel()

                    try await clientChannel.executeThenClose { inbound, outbound in
                        var iterator = inbound.makeAsyncIterator()
                        let reuse = args.connectionReuse?.randomElement()
                        var executed = 0
                        while reuse != nil ? executed <= reuse! && end > now() : end > now() {
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

    private func runConstantThroughput(head: HTTPRequestHead) async {
        let throughputPerConnection = args.throughput / args.concurrency

        @Sendable func tick() async {
            await withDiscardingTaskGroup { connectionGroup in
                for _ in 0..<args.concurrency {
                    connectionGroup.addTask {
                        var actualThroughput = 0
                        do {
                            func runConnection() async throws {
                                let channel = try await makeHTTP1Channel()
                                try await channel.executeThenClose { inbound, outbound in
                                    var iterator = inbound.makeAsyncIterator()
                                    for _ in 0..<(args.connectionReuse?.randomElement() ?? throughputPerConnection) {
                                        if actualThroughput >= throughputPerConnection {
                                            throw CancellationError()
                                        }
                                        do {
                                            try await executeRequest(start: now(), head: head, outbound: outbound, inboundIterator: &iterator)
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

    private func executeRequest(
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

extension BenchmarkRunner {

    private func makeHTTP1Channel() async throws -> NIOAsyncChannel<HTTPClientResponsePart, HTTPPart<HTTPRequestHead, ByteBuffer>> {
        try await bootstrap.connect(target: target) { channel in
            channel.eventLoop.makeCompletedFuture {
                if let sslContext {
                    let sslHandler = try NIOSSLClientHandler(context: sslContext, serverHostname: target.sslServerHostname)
                    try channel.pipeline.syncOperations.addHandler(sslHandler)
                }
                try channel.pipeline.syncOperations.addHTTPClientHandlers()
                try channel.pipeline.syncOperations.addHandler(HTTPByteBufferResponsePartHandler())
                return try NIOAsyncChannel(wrappingChannelSynchronously: channel)
            }
        }
    }

    private static func makeBootstrap(timeout: Int64, on eventLoopGroup: any EventLoopGroup) -> any ConnectionTargetBootstrap {
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

extension BenchmarkRunner {
    private final class HTTPByteBufferResponsePartHandler: ChannelOutboundHandler {
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
