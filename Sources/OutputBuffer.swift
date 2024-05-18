import NIOCore
import NIOPosix

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
