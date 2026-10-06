import Foundation
import Network
import Testing

@testable import CrabfleetMac

struct RelayHostPublisherTests {
  @Test
  func largeConsumeDropsRetainedStorage() {
    var buffer = RelayIncomingBuffer()
    let size = 2_000_000
    buffer.append(Data(count: size))
    #expect(buffer.retainedStartIndex == 0)
    #expect(buffer.retainedAllocationBytes >= size)

    let consumed = buffer.consume(1_500_000)
    #expect(consumed.count == 1_500_000)
    #expect(buffer.count == 500_000)
    #expect(buffer.retainedStartIndex == 0)
    #expect(buffer.retainedAllocationBytes > 0)
    #expect(buffer.retainedAllocationBytes < 1_000_000)

    _ = buffer.consume(buffer.count)
    #expect(buffer.count == 0)
    #expect(buffer.retainedStartIndex == 0)
    #expect(buffer.retainedAllocationBytes == 0)
  }

  @Test
  func websocketByteStreamReassemblesReadsAndChunksWrites() async throws {
    let task = RecordingRelayWebSocketTask(incoming: [
      .data(Data([1, 2])),
      .data(Data([3, 4, 5])),
    ])
    let stream = RelayWebSocketByteStream(task: task)

    try await stream.waitForIncomingData()
    #expect(try await stream.readExactly(4) == Data([1, 2, 3, 4]))
    #expect(try await stream.readExactly(1) == Data([5]))

    let payload = Data(repeating: 0x2a, count: RelayWebSocketByteStream.sendChunkBytes * 2 + 1)
    try await stream.send(payload)
    #expect(task.sentData.map(\.count) == [256 * 1_024, 256 * 1_024, 1])
    #expect(task.sentData.reduce(into: Data()) { $0.append($1) } == payload)
  }

  @Test
  func websocketByteStreamRejectsTextAndOversizedMessages() async {
    let text = RelayWebSocketByteStream(
      task: RecordingRelayWebSocketTask(incoming: [.string("not RFB")])
    )
    await #expect(throws: (any Error).self) {
      _ = try await text.readExactly(1)
    }

    let oversized = RelayWebSocketByteStream(
      task: RecordingRelayWebSocketTask(incoming: [
        .data(Data(count: RelayWebSocketByteStream.maximumMessageBytes + 1))
      ])
    )
    await #expect(throws: (any Error).self) {
      _ = try await oversized.readExactly(1)
    }
  }

  @Test
  func websocketByteStreamDropsExpiredDeadlineSends() async {
    let task = RecordingRelayWebSocketTask(incoming: [])
    let stream = RelayWebSocketByteStream(task: task)

    await #expect(throws: RFBSendExpiredError.self) {
      try await stream.send(Data([1]), deadline: ContinuousClock().now)
    }
    #expect(task.sentData.isEmpty)
  }

  @Test
  func relaySessionSendsServerBannerBeforeWaitingForViewerBytes() async throws {
    let clientBanner = Data("RFB 003.008\n".utf8)
    let task = RecordingRelayWebSocketTask(incoming: [.data(clientBanner)])
    let stream = SessionClaimingRFBByteStream(
      base: RelayWebSocketByteStream(task: task),
      gate: RFBHostSessionGate(),
      onAcquire: {},
      onRelease: {}
    )

    try await stream.send(RFBVersion.serverBanner)
    #expect(task.sentData == [RFBVersion.serverBanner])
    #expect(try await stream.readExactly(12) == clientBanner)
    stream.finishHandshake()
    stream.finishClaim()
  }

  @Test
  func onlyAuthenticatedRelayPublisherSessionKeepsSecurityNoneBypass() async throws {
    var viewerHandshake = RFBVersion.serverBanner
    viewerHandshake.append(contentsOf: [1, 1])  // None selection, shared ClientInit.
    let task = RecordingRelayWebSocketTask(incoming: [.data(viewerHandshake)])
    let descriptor = CapturedDisplayDescriptor(
      displayID: 1,
      displayBounds: CGRect(x: 0, y: 0, width: 64, height: 64),
      frameWidth: 64,
      frameHeight: 64,
      sourcePixelWidth: 64,
      sourcePixelHeight: 64)
    let publisher = RelayHostPublisher(
      endpoint: try #require(URL(string: "wss://fleet.example.test/relay")),
      relayAccess: "test-auth-token",
      capture: MacScreenCapture(),
      descriptor: descriptor,
      input: RelayNoopInput(),
      clipboard: nil,
      sessionGate: RFBHostSessionGate(),
      eventHandler: { _ in },
      taskFactory: { _ in task })

    publisher.start()
    defer { publisher.stop() }
    let deadline = ContinuousClock().now.advanced(by: .seconds(2))
    while task.sentData.count < 4, ContinuousClock().now < deadline {
      try await Task.sleep(for: .milliseconds(10))
    }

    let sent = task.sentData
    #expect(sent.count >= 4)
    guard sent.count >= 4 else { return }
    #expect(sent[0] == RFBVersion.serverBanner)
    #expect(sent[1] == Data([1, 1]))
    #expect(sent[2] == Data([0, 0, 0, 0]))
  }

  @Test
  func registrationBuildsSecureRelayEndpoint() throws {
    let registration = try #require(
      CrabfleetDesktopRegistration(environment: [
        "CRABFLEET_API_URL": "https://fleet.example",
        "CRABFLEET_SESSION_COOKIE": "test-cookie-placeholder",
      ])
    )

    #expect(
      registration.relayHostURL(hostID: "studio")?.absoluteString
        == "wss://fleet.example/api/desktop-hosts/studio/relay/host"
    )
  }

  @Test @MainActor
  func browserRelayDefaultsOnAndPersistsUserChoice() throws {
    let suiteName = "CrabfleetMacTests.\(UUID().uuidString)"
    let defaults = try #require(UserDefaults(suiteName: suiteName))
    defaults.removePersistentDomain(forName: suiteName)
    let controller = PrivateMacShareController(
      runner: StaticRelayTailscaleRunner(),
      desktopRegistration: nil,
      defaults: defaults
    )

    #expect(controller.browserAccessEnabled)
    controller.browserAccessEnabled = false
    #expect(defaults.object(forKey: PrivateMacShareController.browserAccessDefaultsKey) as? Bool == false)
  }

  @Test
  func urlSessionRelayReadsBinaryFrames() async throws {
    let payload = Data((0..<400_000).map { UInt8($0 % 251) })
    let server = LocalBinaryWebSocket()
    let port = try await server.listen()
    defer { server.stop() }
    let session = URLSession(configuration: .ephemeral)
    defer { session.invalidateAndCancel() }
    let task = session.webSocketTask(with: URL(string: "ws://127.0.0.1:\(port)/relay")!)
    let stream = RelayWebSocketByteStream(task: task)
    task.resume()
    try await server.send(Data(payload.prefix(200_000)))
    try await server.send(Data(payload.dropFirst(200_000)))
    let started = DispatchTime.now().uptimeNanoseconds
    let first = try await stream.readExactly(250_000)
    let second = try await stream.readExactly(150_000)
    let milliseconds = (DispatchTime.now().uptimeNanoseconds - started) / 1_000_000
    #expect(first + second == payload)
    print(
      "{\"event\":\"relay_url_session\",\"port\":\(port),\"frames\":2,\"readBytes\":\(first.count + second.count),\"milliseconds\":\(milliseconds)}"
    )
    stream.cancel()
  }
}

private final class LocalBinaryWebSocket: @unchecked Sendable {
  private let lock = NSLock()
  private var listener: NWListener?
  private var connection: NWConnection?
  private var isReady = false
  private var readyWaiter: CheckedContinuation<Void, Error>?

  func listen() async throws -> UInt16 {
    let socketOptions = NWProtocolWebSocket.Options()
    socketOptions.autoReplyPing = true
    let parameters = NWParameters.tcp
    parameters.defaultProtocolStack.applicationProtocols.insert(socketOptions, at: 0)
    let listener = try NWListener(using: parameters, on: .any)
    self.listener = listener
    listener.newConnectionHandler = { [weak self] connection in
      self?.accept(connection)
    }
    return try await withCheckedThrowingContinuation { continuation in
      listener.stateUpdateHandler = { state in
        switch state {
        case .ready:
          continuation.resume(returning: listener.port?.rawValue ?? 0)
          listener.stateUpdateHandler = { _ in }
        case .failed(let error):
          continuation.resume(throwing: error)
          listener.stateUpdateHandler = { _ in }
        default:
          break
        }
      }
      listener.start(queue: .global())
    }
  }

  func send(_ data: Data) async throws {
    try await waitUntilReady()
    guard let connection else { throw URLError(.cannotConnectToHost) }
    let metadata = NWProtocolWebSocket.Metadata(opcode: .binary)
    let context = NWConnection.ContentContext(identifier: "binary", metadata: [metadata])
    try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
      connection.send(
        content: data,
        contentContext: context,
        isComplete: true,
        completion: .contentProcessed { error in
          if let error {
            continuation.resume(throwing: error)
          } else {
            continuation.resume()
          }
        }
      )
    }
  }

  func stop() {
    connection?.cancel()
    listener?.cancel()
  }

  private func accept(_ connection: NWConnection) {
    connection.stateUpdateHandler = { [weak self] state in
      guard let self else { return }
      switch state {
      case .ready:
        self.lock.lock()
        self.isReady = true
        let waiter = self.readyWaiter
        self.readyWaiter = nil
        self.lock.unlock()
        waiter?.resume()
      case .failed(let error):
        self.lock.lock()
        let waiter = self.readyWaiter
        self.readyWaiter = nil
        self.lock.unlock()
        waiter?.resume(throwing: error)
      default:
        break
      }
    }
    lock.lock()
    self.connection = connection
    lock.unlock()
    connection.start(queue: .global())
  }

  private func waitUntilReady() async throws {
    if readyNow() { return }
    try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
      if installWaiter(continuation) {
        continuation.resume()
      }
    }
  }

  private func readyNow() -> Bool {
    lock.lock()
    defer { lock.unlock() }
    return isReady
  }

  private func installWaiter(_ continuation: CheckedContinuation<Void, Error>) -> Bool {
    lock.lock()
    defer { lock.unlock() }
    if isReady { return true }
    readyWaiter = continuation
    return false
  }
}

private struct RelayNoopInput: RemoteInputForwarding {
  func keyEvent(down: Bool, keysym: UInt32) {}
  func pointerEvent(buttonMask: UInt8, x: UInt16, y: UInt16) {}
}

private final class RecordingRelayWebSocketTask: RelayWebSocketTasking, @unchecked Sendable {
  private let lock = NSLock()
  private var incoming: [URLSessionWebSocketTask.Message]
  private var sent = [Data]()

  init(incoming: [URLSessionWebSocketTask.Message]) {
    self.incoming = incoming
  }

  var sentData: [Data] { withLock { sent } }

  func resume() {}

  func receive() async throws -> URLSessionWebSocketTask.Message {
    try withLock {
      guard !incoming.isEmpty else {
        throw PrivateMacShareError.protocolError("test websocket ended")
      }
      return incoming.removeFirst()
    }
  }

  func send(_ message: URLSessionWebSocketTask.Message) async throws {
    guard case .data(let data) = message else {
      throw PrivateMacShareError.protocolError("test expected binary data")
    }
    withLock { sent.append(data) }
  }

  func cancel(with closeCode: URLSessionWebSocketTask.CloseCode, reason: Data?) {}

  private func withLock<T>(_ body: () throws -> T) rethrows -> T {
    lock.lock()
    defer { lock.unlock() }
    return try body()
  }
}

private struct StaticRelayTailscaleRunner: TailscaleCommandRunning {
  func run(arguments: [String]) async throws -> TailscaleCommandResult {
    throw PrivateMacShareError.tailscaleOffline
  }
}
