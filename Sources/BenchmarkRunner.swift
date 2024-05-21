import Atomics
import Histogram
import NIOConcurrencyHelpers
import NIOCore
import NIOPosix
import NIOHTTP1
import NIOHTTP2
import NIOSSL
#if canImport(Network)
import NIOTransportServices
#endif

protocol Arguments {
    var concurrency: Int { get }
    var throughput: Int { get }
    var timeout: Int64 { get }
    var connectionReuse: Range<Int>? { get }
    var http2: Bool { get }
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

    private let eventLoopGroup: any EventLoopGroup
    private let bootstrap: any ConnectionTargetBootstrap
    private let bufferAllocator = ByteBufferAllocator()

    private let sslContext: NIOSSLContext?

    init(
        _ args: Arguments,
        clock: Clock,
        start: Clock.Instant,
        end: Clock.Instant,
        on eventLoopGroup: any EventLoopGroup
    ) throws {
        self.args = args
        self.clock = clock
        self.start = start
        self.end = end
        self.eventLoopGroup = eventLoopGroup

        let (scheme, target, uri) = try deconstructURL(args.url)
        self.target = target
        self.uri = uri

        let sslContext: NIOSSLContext?
        switch scheme {
        case .https, .httpsUnix:
            var configuration = TLSConfiguration.clientDefault
            // might be worth adding customisation abilities in the future...
            configuration.certificateVerification = .none
            if args.http2 {
                configuration.applicationProtocols = ["h2"]
            }
            sslContext = try NIOSSLContext(configuration: configuration)
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
        let head = HTTPRequestHead(version: args.http2 ? .http2 : .http1_1, method: .GET, uri: uri, headers: ["Host": target.host ?? ""])
        while end > clock.now {
            if args.throughput != 0 {
                await runConstantThroughput(head: head)
            } else {
                await runConstantConcurrency(head: head)
            }
        }

        let duration = start.duration(to: clock.now)
        let total = total.load(ordering: .relaxed)
        return BenchmarkResult(
            duration: duration,
            total: total,
            errors: errors.load(ordering: .relaxed),
            throughput: Double(total) / Double(duration.components.seconds),
            histogram: histogram.withLockedValue({ $0 })
        )
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


// MARK: Constant Concurrency

extension BenchmarkRunner {

    private func runConstantConcurrency(head: HTTPRequestHead) async {
        if args.http2 {
            do {
                try await runConstantConcurrency_HTTP2(head: head)
            } catch {
                print(error)
                errors.wrappingIncrement(ordering: .relaxed)
            }
            return
        }

        await withDiscardingTaskGroup { group in
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
                            while reuse != nil ? executed <= reuse! && end > clock.now : end > clock.now {
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
    }

    private func runConstantConcurrency_HTTP2(head: HTTPRequestHead) async throws {
        let channel = try await makeHTTP2Channel()
        let reuse = args.connectionReuse?.randomElement()
        let executed = ManagedAtomic(0)
        while reuse != nil ? executed.load(ordering: .relaxed) <= reuse! && end > clock.now : end > clock.now {
            try await withThrowingDiscardingTaskGroup { group in
                for _ in 0..<args.concurrency {
                    group.addTask {
                        let start = clock.now
                        let stream = try await openHTTP2Stream(on: channel)
                        try await stream.executeThenClose { inbound, outbound in
                            var iterator = inbound.makeAsyncIterator()
                            try await executeRequest(start: start, head: head, outbound: outbound, inboundIterator: &iterator)
                        }
                        executed.wrappingIncrement(ordering: .relaxed)
                    }
                }
            }
        }
    }

}


// MARK: Constant Throughput

extension BenchmarkRunner {

    private func runConstantThroughput(head: HTTPRequestHead) async {
        let throughputPerConnection = args.throughput / args.concurrency

        @Sendable func tick() async {
            if args.http2 {
                await runConstantThroughput_tick_HTTP2(throughputPerConnection: throughputPerConnection, head: head)
            } else {
                await runConstantThroughput_tick_HTTP1(throughputPerConnection: throughputPerConnection, head: head)
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

    private func runConstantThroughput_tick_HTTP1(throughputPerConnection: Int, head: HTTPRequestHead) async {
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
                                        try await executeRequest(start: clock.now, head: head, outbound: outbound, inboundIterator: &iterator)
                                        actualThroughput += 1
                                    } catch is CancellationError {
                                        throw CancellationError() // cascade
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

    private func runConstantThroughput_tick_HTTP2(throughputPerConnection: Int, head: HTTPRequestHead) async {
        await withDiscardingTaskGroup { connectionsGroup in
            let channel: NIOHTTP2Handler.AsyncStreamMultiplexer<Void>
            do {
                channel = try await makeHTTP2Channel()
            } catch {
                print(error)
                errors.wrappingIncrement(ordering: .relaxed)
                return
            }

            for _ in 0..<args.concurrency {
                connectionsGroup.addTask {
                    let actualThroughput = ManagedAtomic(0)
                    do {
                        @Sendable func runRequest() async throws {
                            let start = clock.now
                            let stream = try await openHTTP2Stream(on: channel)
                            try await stream.executeThenClose { inbound, outbound in
                                var iterator = inbound.makeAsyncIterator()
                                try await executeRequest(start: start, head: head, outbound: outbound, inboundIterator: &iterator)
                            }
                        }

                        while actualThroughput.load(ordering: .relaxed) < throughputPerConnection {
                            try await withThrowingDiscardingTaskGroup { singleConnectionGroup in
                                singleConnectionGroup.addTask {
                                    for _ in 0..<(args.connectionReuse?.randomElement() ?? throughputPerConnection) {
                                        if actualThroughput.load(ordering: .relaxed) >= throughputPerConnection {
                                            throw CancellationError()
                                        }
                                        do {
                                            try await runRequest()
                                            actualThroughput.wrappingIncrement(ordering: .relaxed)
                                        } catch is CancellationError {
                                            throw CancellationError() // cascade
                                        } catch {
                                            print(error)
                                            errors.wrappingIncrement(ordering: .relaxed)
                                        }
                                    }
                                }
                            }
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

    private func makeHTTP2Channel() async throws -> NIOHTTP2Handler.AsyncStreamMultiplexer<Void> {
        try await bootstrap.connect(target: target) { channel in
            return channel.eventLoop.makeCompletedFuture {
                if let sslContext {
                    let sslHandler = try NIOSSLClientHandler(context: sslContext, serverHostname: target.sslServerHostname)
                    try channel.pipeline.syncOperations.addHandler(sslHandler)
                }
                return try channel.pipeline.syncOperations.configureAsyncHTTP2Pipeline(mode: .client) {
                    $0.eventLoop.makeSucceededVoidFuture()
                }
            }
        }
    }

    private func openHTTP2Stream(on channel: NIOHTTP2Handler.AsyncStreamMultiplexer<Void>) async throws -> NIOAsyncChannel<HTTPClientResponsePart, HTTPPart<HTTPRequestHead, ByteBuffer>> {
        try await channel.openStream { channel in
            channel.eventLoop.makeCompletedFuture {
                try channel.pipeline.syncOperations.addHandlers([HTTP2FramePayloadToHTTP1ClientCodec(httpProtocol: .https), HTTPByteBufferResponsePartHandler()])
                return try NIOAsyncChannel<HTTPClientResponsePart, HTTPPart<HTTPRequestHead, ByteBuffer>>(wrappingChannelSynchronously: channel)
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

extension HTTPHeaders {
    fileprivate init(responseHead: HTTPResponseHead) {
        // To avoid too much allocation we create an array first, and then initialize the HTTPHeaders from it.
        // This array will need to be the size of the response headers + 1, for the :status field.
        var newHeaders: [(String, String)] = []
        newHeaders.reserveCapacity(responseHead.headers.count + 1)
        newHeaders.append((":status", String(responseHead.status.code)))
        responseHead.headers.forEach { newHeaders.append(($0.name, $0.value)) }

        self.init(newHeaders)
    }
}
