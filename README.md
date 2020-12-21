# airs-ibusnats
[![codecov](https://codecov.io/gh/untillpro/airs-ibusnats/branch/master/graph/badge.svg?token=HmtGrmC6C1)](https://codecov.io/gh/untillpro/airs-ibusnats)

NATS implementation of ibus interface

# Request-response scenario

2 types of communication: single response and sections.
- The requester side: `resp, sections, secErr, err := ibus.SendRequest2()`.
  - Communication type will be determined under the hood by the last byte of the first received packet (see packet binary format description below):
    - `ibus.BusPacketResponse` -> the previous bytes of the packet are serialized `ibus.Response` (see  serialization format description below). Deserialize it, unsubscribe from inbox, return.
    - otherwise -> sectioned communication. Previous bytes of the packet are section header + first element or `close` packet. Launch further packets reading and sending to `sections` channel in a separate goroutine. Further packets are new section header + first element or next element or `close` packet. `close` packet received or timeout of the whole communication -> unsubscribe from inbox, close sections channel, finalize the goroutine
  - caller determines the communication type choised by handler by `sections`:
    - nil -> single response. `sections` and `secErr` must be igonred
    - non-nil -> sectioned response. `resp` must be ignored
      - `sections` must be readed till the end
      - `secErr` contains read error after `sections` close
  - `err` contains NATS-related errors occured before or during receiving the first response packet. `err` not nil -> impossible to determine the communication type
- The handler side: `ibus.RequestHandler()` is called by bus
   - call `ibus.SendResponse()` to send a single `ibus.Response`
     - then call `ibus.SendResponse()` again or `ibus.SendParallelResponse()` and use `ibus.IResultSenderCloseable` methods -> nothing happens, no error. New response is actually sent to NATS but skipped because publisher will unsubscribe from the topic after the first response packet receive
   - call `ibus.SendParallelResponse2()` to send sectioned response, then use `ibus.IResultSenderCloseable` methods
     - then call `ibus.SendParallelResponse2()` again and use methods of new `ibus.IResultSenderCloseable` - no error, requester will receive sections as the handler sent. The same behaviour is after `.Close()`
     - then call `ibus.SendResponse()` - nothing happens. Packet will be received bye requester and skipped because it is not section-related
     - then call `ibus.SendResponse()` after `.Close()` - nothing happens, no error. Packet actually will be sent to NATS and skipped because the requester will unsubscribe from the topic after cClose`

# Service properties
- `Verbose` true -> raw NATS traffic will be logged
- `NATSServer` `;`-separated string -> list of NATS servers e.g. `nats://127.0.0.1:4222`
- `Parts`- parts the partition numbers range is divided on.
- `CurrentPart` - partition numbers range complied to `CurrentPart`of `Parts` will be used by the service
- `CurrentQueueName` - queue name subscribers are read from
- `Queues` map[partition]queueName
  - defines which partition numbers are available on a certain API entrypoint (e.g. `air-bp`)
  - for `air-bp2`: `air-bp:100`

Subscribers is defined by Queue value. There is only one `Publisher`.
Each NATS subject is Partition


# Timeouts
Generic _read_ timeout is provided to `ibus.SendRequest2()`. It used as:
- timeout of each NATS read operation
- timeout of the whole responsing, i.e. reading until `ibus.Response` or `close` packet is received

Timeout of next response packet awaiting in NATS inbox -> `NextMsg failed: Timeout expired` error
No `close` packet during timeout on sectioned communication -> further reading is terminated, `sectioned communication is terminated: Timeout expired` error

both `airs-router2` and `narrator` uses `ibus.DefaultTimeout` - 10 seconds.

# Bus packets binary formats
  `[]` means optional data
  `elemName` exists for `Map` section only

- `ibus.Response`:

| 1xlen(contentType) | [contentType] | 2xStatusCode | [data] |1xBusPacketType |
|-|-|-|-|-|-|

- Section + first element

| elemBytes | [elemName] | [1xLen(elemName)] | sectionType | 1xlen(sectionType) | path[0] | 1xLen(path[0]) | ... | 1xLen([]path) | 1xBusPacketType |
|-|-|-|-|-|-|-|-|-|-|

- Next element

| elemBytes | [elemName] | [1xLen(elemName)] | 1xBusPacketType |
|-|-|-|-|

- Close

| [error message] | 1xBusPacketType |
|-|-|

# Limitations
- `ibus.IResultSenderCloseable`:
  - `sectionType`, `path` elem and `elementName` max length is 255 chars
  - `[]path` max length is 255 elements
- `IResultSenderCloseabe.Close()`: failed to send to NATS -> no error, nothing happens, just error is logged. Requester will get `*secErr` saying that communication took too much time (no sections or elements or `close` for a long time)
- `ibus.SendResponse()`:
  - error on publish -> nothing happens, just error is logged
  - wrong sender (not `senderImpl`) -> nothing happens, just error logged
- failed to unsubscribe from the queue on service stop -> nothing happens, just error is logged
