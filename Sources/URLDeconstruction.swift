import struct Foundation.URL
import struct Foundation.URLComponents

extension URL {
    var percentEncodedPath: String {
        if self.path.isEmpty {
            return "/"
        }
        return URLComponents(
            url: self,
            resolvingAgainstBaseURL: false
        )?.percentEncodedPath ?? self.path
    }

    var uri: String {
        var uri = self.percentEncodedPath

        if let query = self.query {
            uri += "?" + query
        }

        return uri
    }
}

enum URLDeconstructionError: Error {
    case invalidURL
    case emptyScheme
    case unsupportedScheme(String)
    case emptyHost
    case missingSocketPath
}

enum Scheme: String {
    case http
    case https
    case unix
    case httpUnix = "http+unix"
    case httpsUnix = "https+unix"
}

extension Scheme {
    var usesTLS: Bool {
        switch self {
        case .http, .httpUnix, .unix:
            return false
        case .https, .httpsUnix:
            return true
        }
    }

    var defaultPort: Int {
        self.usesTLS ? 443 : 80
    }
}

typealias URI = String

func deconstructURL(_ url: String) throws /* (URLDeconstructionError) */ -> (Scheme, ConnectionTarget, URI) {
    guard let url = URL(string: url) else {
        throw URLDeconstructionError.invalidURL
    }
    guard let rawScheme = url.scheme else {
        throw URLDeconstructionError.emptyScheme
    }
    guard let scheme = Scheme(rawValue: rawScheme.lowercased()) else {
        throw URLDeconstructionError.unsupportedScheme(rawScheme)
    }

    switch scheme {
    case .http, .https:
        guard let host = url.host, !host.isEmpty else {
            throw URLDeconstructionError.emptyHost
        }
        return (
            scheme,
            .init(remoteHost: host, port: url.port ?? scheme.defaultPort),
            url.uri
        )

    case .unix:
        let socketPath = url.baseURL?.path ?? url.path
        let uri = url.baseURL != nil ? url.uri : "/"
        guard !socketPath.isEmpty else {
            throw URLDeconstructionError.missingSocketPath
        }
        return (scheme, .unixSocket(path: socketPath), uri)

    case .httpUnix, .httpsUnix:
        guard let socketPath = url.host, !socketPath.isEmpty else {
            throw URLDeconstructionError.missingSocketPath
        }
        return (scheme, .unixSocket(path: socketPath), url.uri)
    }
}
