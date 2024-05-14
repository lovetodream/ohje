import ArgumentParser
import NIOPosix
import NIOHTTP1
import NIOHTTP2

@main
struct App: AsyncParsableCommand {
    @Option(name: .shortAndLong)
    var threads: Int = 1

    @Option
    var timeout: Int64 = 30

    @Option(name: .shortAndLong)
    var rate: Int

    @Argument
    var url: String

    func run() async throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: threads)
        let client = HTTPClient(eventLoopGroup: eventLoopGroup)
        let clock = ContinuousClock()

        await withDiscardingTaskGroup { group in
            for _ in 0..<rate {
                group.addTask {
                    let start = clock.now
                    await execute(on: client)
                    let duration = start.duration(to: .now)
                    print(duration)
                }
            }
        }

        try await client.shutdown()
        try await eventLoopGroup.shutdownGracefully()
    }

    func execute(on client: HTTPClient) async {
        do {
            let response = try await client.execute(.init(url: url), timeout: .seconds(timeout))
            if response.status.code / 100 != 2 { } // TODO: fail
        } catch {
            // TODO: fail
        }
    }
}
