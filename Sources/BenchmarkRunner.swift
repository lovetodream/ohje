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
    let total: UInt64
    let connectErrors: UInt64
    let readErrors: UInt64
    let writeErrors: UInt64
    let timeoutErrors: UInt64
    let httpResponseErrors: UInt64
    let bytesWritten: UInt64
    let bytesRead: UInt64
    let throughput: Double
    let latency: Histogram<UInt64>
    let requests: Histogram<UInt64>
}

struct BenchmarkRunner<Clock: _Concurrency.Clock> where Clock.Duration == Duration {
    private let args: Arguments
    private let target: ConnectionTarget
    private let uri: String

    private let total = ManagedAtomic(UInt64(0))
    private let currentRequests = ManagedAtomic(UInt64(0))
    private let connectErrors = ManagedAtomic(UInt64(0))
    private let readErrors = ManagedAtomic(UInt64(0))
    private let writeErrors = ManagedAtomic(UInt64(0))
    private let timeoutErrors = ManagedAtomic(UInt64(0))
    private let httpResponseErrors = ManagedAtomic(UInt64(0))
    private let bytesWritten: ManagedAtomic<UInt64>
    private let bytesRead: ManagedAtomic<UInt64>
    private let latency = NIOLockedValueBox(Histogram<UInt64>(highestTrackableValue: 60_000_000)) // max: 1 min in usec
    private let requests = NIOLockedValueBox(Histogram<UInt64>(numberOfSignificantValueDigits: .three))

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

        let bytesWritten = ManagedAtomic(UInt64(0))
        let bytesRead = ManagedAtomic(UInt64(0))
        self.bytesWritten = bytesWritten
        self.bytesRead = bytesRead
        self.bootstrap = Self.makeBootstrap(timeout: args.timeout, on: eventLoopGroup, handlers: {
            [WireReader(bytesWritten: bytesWritten, bytesRead: bytesRead)]
        })
    }

    func shutdown() async throws {
        try await self.eventLoopGroup.shutdownGracefully()
    }

    func run() async throws -> BenchmarkResult {
        let head = HTTPRequestHead(version: args.http2 ? .http2 : .http1_1, method: .GET, uri: uri, headers: ["Host": target.host ?? ""])
        await withDiscardingTaskGroup { group in
            group.addTask {
                var start = clock.now
                for await _ /* tick */ in AsyncTimerSequence.repeating(every: .seconds(1), clock: clock) {
                    sampleRate(start: &start)
                }
            }

            while end > clock.now {
                if args.throughput != 0 {
                    await runConstantThroughput(head: head, group: &group)
                } else {
                    await runConstantConcurrency(head: head, group: &group)
                }
            }
        }

        let duration = start.duration(to: clock.now)
        let total = total.load(ordering: .relaxed)
        return BenchmarkResult(
            duration: duration,
            total: total,
            connectErrors: connectErrors.load(ordering: .relaxed),
            readErrors: readErrors.load(ordering: .relaxed),
            writeErrors: writeErrors.load(ordering: .relaxed),
            timeoutErrors: timeoutErrors.load(ordering: .relaxed),
            httpResponseErrors: httpResponseErrors.load(ordering: .relaxed),
            bytesWritten: bytesWritten.load(ordering: .relaxed),
            bytesRead: bytesRead.load(ordering: .relaxed),
            throughput: Double(total) / Double(duration.components.seconds),
            latency: latency.withLockedValue({ $0 }),
            requests: requests.withLockedValue({ $0 })
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
                let status = head.status.code / 100
                if status != 2 && status != 3 {
                    throw HTTPResponseError()
                }
            case .body: continue
            case .end:
                let duration = start.duration(to: clock.now).asMicroseconds()
                latency.withLockedValue { _ = $0.record(duration) }
                currentRequests.wrappingIncrement(ordering: .relaxed)
                return
            }
        }
    }

    struct ErrorHandled: Error { }
    enum ErrorHandling {
        case rethrow(as: any Error)
        case `continue`
    }
    @discardableResult
    private func handleError(_ error: any Error) -> ErrorHandling {
        if error is HTTPResponseError {
            httpResponseErrors.wrappingIncrement(ordering: .relaxed)
            readErrors.wrappingIncrement(ordering: .relaxed)
            return .continue
        } else if let error = error as? ChannelError {
            switch error {
            case .connectPending, .unknownLocalAddress, .badMulticastGroupAddressFamily, .badInterfaceAddressFamily, .illegalMulticastAddress, .multicastNotSupported, .operationUnsupported, .inappropriateOperationForState, .unremovableHandler:
                connectErrors.wrappingIncrement(ordering: .relaxed)
            case .connectTimeout:
                timeoutErrors.wrappingIncrement(ordering: .relaxed)
            case .ioOnClosedChannel, .alreadyClosed, .outputClosed, .writeMessageTooLarge, .writeHostUnreachable:
                writeErrors.wrappingIncrement(ordering: .relaxed)
            case .inputClosed, .eof:
                readErrors.wrappingIncrement(ordering: .relaxed)
            }
            return .rethrow(as: ErrorHandled())
        } else if let error = error as? NIOAsyncWriterError {
            if error == .alreadyFinished() {
                writeErrors.wrappingIncrement(ordering: .relaxed)
            }
            return .rethrow(as: ErrorHandled())
        } else {
            fatalError("TODO: handle me: \(error)")
        }
    }

    private func sampleRate(start: inout Clock.Instant) {
        let elapsedTime = start.duration(to: clock.now)
        let elapsedMilliseconds = elapsedTime.components.seconds * 1000 + elapsedTime.components.attoseconds / 1_000_000_000_000_000
        start = clock.now
        let rawRequests = currentRequests.exchange(0, ordering: .relaxed)
        let requests = (Double(rawRequests) / Double(elapsedMilliseconds)) * 1000.0
        total.wrappingIncrement(by: rawRequests, ordering: .relaxed)
        self.requests.withLockedValue { _ = $0.record(UInt64(requests)) }
    }
}


// MARK: Constant Concurrency

extension BenchmarkRunner {

    private func runConstantConcurrency(head: HTTPRequestHead, group: inout DiscardingTaskGroup) async {
        if args.http2 {
            do {
                try await runConstantConcurrency_HTTP2(head: head)
            } catch {
                handleError(error)
            }
            return
        }

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
                    handleError(error)
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

    private func runConstantThroughput(head: HTTPRequestHead, group: inout DiscardingTaskGroup) async {
        let throughputPerConnection = args.throughput / args.concurrency

        @Sendable func tick() async {
            if args.http2 {
                await runConstantThroughput_tick_HTTP2(throughputPerConnection: throughputPerConnection, head: head)
            } else {
                await runConstantThroughput_tick_HTTP1(throughputPerConnection: throughputPerConnection, head: head)
            }
        }

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
                                        switch handleError(error) {
                                        case .continue:
                                            continue
                                        case .rethrow(as: let new):
                                            throw new
                                        }
                                    }
                                }
                            }
                        }

                        while actualThroughput < throughputPerConnection {
                            try await runConnection()
                        }
                    } catch is CancellationError {
                        return
                    } catch is ErrorHandled {
                        return
                    } catch {
                        handleError(error)
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
                handleError(error)
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
                                            switch handleError(error) {
                                            case .continue:
                                                continue
                                            case .rethrow(as: let new):
                                                throw new
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    } catch is CancellationError {
                        return
                    } catch is ErrorHandled {
                        return
                    } catch {
                        handleError(error)
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

    private static func makeBootstrap(
        timeout: Int64,
        on eventLoopGroup: any EventLoopGroup,
        handlers: @escaping @Sendable () -> [any ChannelHandler]
    ) -> any ConnectionTargetBootstrap {
        #if canImport(Network)
        if let tsBootstrap = NIOTSConnectionBootstrap(validatingGroup: eventLoopGroup) {
            return tsBootstrap
                .connectTimeout(.seconds(timeout))
                .channelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
                .channelOption(ChannelOptions.tcpOption(.tcp_nodelay), value: 1)
                .protocolHandlers(handlers)
        }
        #endif

        guard let bootstrap = ClientBootstrap(validatingGroup: eventLoopGroup) else {
            fatalError("No matching bootstrap found")
        }

        return bootstrap
            .connectTimeout(.seconds(timeout))
            .channelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .channelOption(ChannelOptions.tcpOption(.tcp_nodelay), value: 1)
            .protocolHandlers(handlers)
    }
}


// MARK: Utility

extension BenchmarkRunner {
    private final class WireReader: ChannelDuplexHandler {
        typealias InboundIn = ByteBuffer
        typealias OutboundIn = ByteBuffer

        private let bytesWritten: ManagedAtomic<UInt64>
        private let bytesRead: ManagedAtomic<UInt64>

        init(bytesWritten: ManagedAtomic<UInt64>, bytesRead: ManagedAtomic<UInt64>) {
            self.bytesWritten = bytesWritten
            self.bytesRead = bytesRead
        }

        func channelRead(context: ChannelHandlerContext, data: NIOAny) {
            context.fireChannelRead(data)
            let bytes = unwrapInboundIn(data).readableBytes
            bytesRead.wrappingIncrement(by: UInt64(bytes), ordering: .relaxed)
        }

        func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
            context.write(data, promise: promise)
            let bytes = unwrapOutboundIn(data).readableBytes
            bytesWritten.wrappingIncrement(by: UInt64(bytes), ordering: .relaxed)
        }
    }

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
