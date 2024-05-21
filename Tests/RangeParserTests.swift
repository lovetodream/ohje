import XCTest
@testable import Ohje

final class RangeParserTests: XCTestCase {
    func testAll() {
        XCTAssertEqual(try parseRange("0..<100"), 0..<100)
        XCTAssertEqual(try parseRange("10...50"), 10..<51)
        XCTAssertEqual(try parseRange("20"), 20..<21)
        XCTAssertEqual(try parseRange("1..."), 1..<Int.max)
        XCTAssertEqual(try parseRange("...20"), 0..<21)
        XCTAssertEqual(try parseRange("..<10"), 0..<10)
    }
}
