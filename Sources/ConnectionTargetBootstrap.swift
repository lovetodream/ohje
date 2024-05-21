import NIOCore
import NIOPosix
#if canImport(Network)
import NIOTransportServices
#endif

protocol ConnectionTargetBootstrap: NIOClientTCPBootstrapProtocol {
    func connect<Output: Sendable>(
        target: ConnectionTarget,
        channelInitializer: @escaping @Sendable (Channel) -> EventLoopFuture<Output>
    ) async throws -> Output
}

extension ClientBootstrap: ConnectionTargetBootstrap {
    func connect<Output: Sendable>(
        target: ConnectionTarget,
        channelInitializer: @escaping @Sendable (Channel) -> EventLoopFuture<Output>
    ) async throws -> Output {
        switch target {
        case .ipAddress(_, let socketAddress):
            return try await self.connect(to: socketAddress, channelInitializer: channelInitializer)
        case .domain(let domain, let port):
            return try await self.connect(host: domain, port: port, channelInitializer: channelInitializer)
        case .unixSocket(let path):
            return try await self.connect(unixDomainSocketPath: path, channelInitializer: channelInitializer)
        }
    }
}

#if canImport(Network)
extension NIOTSConnectionBootstrap: ConnectionTargetBootstrap {
    func connect<Output: Sendable>(
        target: ConnectionTarget,
        channelInitializer: @escaping @Sendable (Channel) -> EventLoopFuture<Output>
    ) async throws -> Output {
        switch target {
        case .ipAddress(_, let socketAddress):
            return try await self.connect(to: socketAddress, channelInitializer: channelInitializer)
        case .domain(let domain, let port):
            return try await self.connect(host: domain, port: port, channelInitializer: channelInitializer)
        case .unixSocket(let path):
            return try await self.connect(unixDomainSocketPath: path, channelInitializer: channelInitializer)
        }
    }
}
#endif
