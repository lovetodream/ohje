@Sendable func parseRange(_ value: String) throws /* (InvalidRangeError) */ -> Range<Int> {
    if !value.contains(/(\.\.\.)|(\.\.\<)/), let value = Int(value) {
        return value..<(value + 1)
    } else if value.starts(with: "..."), let value = Int(value.dropFirst("...".count)) {
        return 0..<(value + 1)
    } else if value.starts(with: "..<"), let value = Int(value.dropFirst("..<".count)) {
        return 0..<value
    } else if value.hasSuffix("..."), let value = Int(value.dropLast("...".count)) {
        return value..<Int.max
    } else if value.contains("...") {
        let values = value.split(separator: "...").compactMap { Int($0) }
        if values.count == 2, values[0] <= values[1] {
            return values[0]..<(values[1] + 1)
        }
    } else if value.contains("..<") {
        let values = value.split(separator: "..<").compactMap { Int($0) }
        if values.count == 2, values[0] < values[1] {
            return values[0]..<values[1]
        }
    }
    throw InvalidRangeError(literal: value)
}

struct InvalidRangeError: Error {
    var literal: String
}
