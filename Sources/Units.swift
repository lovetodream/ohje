import Foundation

extension Duration {
    func asMicroseconds() -> UInt64 {
        UInt64(components.attoseconds / 1_000_000_000_000) + UInt64(components.seconds) * 1_000_000
    }
}

enum UnitFormat {
    case binary
    case metric
    case microseconds
    case seconds

    func format(_ value: some BinaryInteger) -> String {
        let value = Double(value)
        switch self {
        case .binary:
            return formatBinary(value, fractionalDigits: 2)
        case .metric:
            return formatMetric(value, fractionalDigits: 0)
        case .microseconds:
            return formatTimeUs(value, fractionalDigits: 2)
        case .seconds:
            return formatTimeS(value, fractionalDigits: 0)
        }
    }

    func format(_ value: Double) -> String {
        switch self {
        case .binary:
            return formatBinary(value)
        case .metric:
            return formatMetric(value)
        case .microseconds:
            return formatTimeUs(value)
        case .seconds:
            return formatTimeS(value)
        }
    }
}

func format(_ value: Double, using: UnitFormat, width: Int = 9, skipPadding: Bool = false) -> String {
    let msg = using.format(value)
    let length = msg.count
    var padding = 2

    if let last = msg.last, last.isLetter { padding -= 1 }
    if length > 1, msg[msg.index(msg.endIndex, offsetBy: -2)].isLetter { padding -= 1 }

    var adjustedWidth = width - padding

    if adjustedWidth < 0 { adjustedWidth = 0 }

    let paddedMsg = String(repeating: " ", count: max(0, adjustedWidth - length)) + msg
    return paddedMsg + String(repeating: " ", count: padding)
}


// MARK: Implementation Details

private struct Units {
    let scale: Int
    let base: String
    let units: [String]
}

private let timeUnitsUs = Units(scale: 1000, base: "μs", units: ["ms", "s"])
private let timeUnitsS = Units(scale: 60, base: "s", units: ["m", "h"])
private let binaryUnits = Units(scale: 1024, base: "B", units: ["KB", "MB", "GB", "TB", "PB"])
private let metricUnits = Units(scale: 1000, base: "", units: ["k", "M", "G", "T", "P"])

private func format(_ value: Double, units: Units, fractionalDigits: Int) -> String {
    var value = value
    var unit = units.base

    let scale = Double(units.scale) * 0.85

    for u in units.units {
        if value >= scale {
            value /= Double(units.scale)
            unit = u
        } else {
            break
        }
    }

    return String(format: "%.\(fractionalDigits)f%@", value, unit)
}

private func formatBinary(_ value: Double, fractionalDigits: Int = 2) -> String {
    return format(value, units: binaryUnits, fractionalDigits: fractionalDigits)
}

private func formatMetric(_ value: Double, fractionalDigits: Int = 2) -> String {
    return format(value, units: metricUnits, fractionalDigits: fractionalDigits)
}

private func formatTimeUs(_ value: Double, fractionalDigits: Int = 2) -> String {
    var units = timeUnitsUs
    var adjustedN = value

    if value >= 1000000.0 {
        adjustedN /= 1000000.0
        units = timeUnitsS
    }
    return format(adjustedN, units: units, fractionalDigits: fractionalDigits)
}

private func formatTimeS(_ value: Double, fractionalDigits: Int = 0) -> String {
    return format(value, units: timeUnitsS, fractionalDigits: fractionalDigits)
}
