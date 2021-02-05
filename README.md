# airs-ibusnats
[![codecov](https://codecov.io/gh/untillpro/airs-ibusnats/branch/master/graph/badge.svg?token=HmtGrmC6C1)](https://codecov.io/gh/untillpro/airs-ibusnats)

NATS implementation of ibus interface

# Bus packets binary formats
```
Response Stream = Response | {MapSection+Elem {Elem} | ArraySection+Elem {Elem} | ObjectSection+Elem} Close | MiscInboxName
Reponse = 0x0 ContentTypeLen_1 ContentType StatusCode_2 Data
MapSection+Elem = 0x2 PathsCount_1 {PathLen_1 Path} LenSectionType_1 SectionType LenElemName_1 ElemName ElemBytes
ArraySection+Elem = 0x3 PathsCount_1 {PathLen_1 Path} LenSectionType_1 SectionType ElemBytes
ObjectSection+Elem = 0x4 PathsCount_1 {PathLen_1 Path} LenSectionType_1 SectionType ElemBytes
Elem = 0x5 [LenElemName_1 ElemName] ElemBytes
Close = 0x1 [ErrorMessage]
MiscInboxName = 0x6 Name
```
```
Misc Stream = GoOn | NoConsumer | SlowConsumer
GoOn = 0x0
NoConsumer = 0x1
SlowConsumer = 0x2
```

# [Slow Consumers](https://docs.nats.io/nats-server/nats_admin/slow_consumers) problem problem solution
- handler creates a temporary inbox and sends its name to requester
- `SendElement` or `ObjectSection` sends an element and waits for a continuation signal from requester side via the temporary inbox
  - `GoOn` packet is received -> return
  - `NoConsumer` packet is received -> `ibusnats.ErrNoConsumer` is returned
  - `SlowConsumer` packet is received -> `ibusnats.SlowConsumer` is returned
  - no messages during `(len(section)/(ibusnats.Service.AllowedSectionKBitsPerSec*1000/8) + 10)` seconds -> `ibus.ErrTimeoutExpired` is returned.
    - examples:
      - AllowedSectionKBitsPerSec = 1000: len(elem or section+elem) 125000 bytes -> 11 seconds max, 250000 bytes -> 12 seconds max etc
  	  - AllowedSectionKBitsPerSec = 100: len(elem or section+elem) 125000 bytes -> 20 seconds max, 250000 bytes -> 30 seconds max etc
- requester reads next section from NATS
  - context done -> send `NoConsumer` notification to the handler, return
  - write section or section element to the channel
    - writting to channel took more than `(len(section)/(ibusnats.Service.AllowedSectionKBitsPerSec*1000/8) + 10)` seconds -> send `SlowConsumer` notification to the handler, terminate communication, return `ibusnats.ErrSlowConsumer` as `*secErr`
      - e.g. router's http client will receive sections and `"status":500,"errorDescription":"section is processed too slow"`
    - otherwise -> send `GoOn` packet to handler, wait for next section from NATS

## Conclusions
- [Slow Consumers](https://docs.nats.io/nats-server/nats_admin/slow_consumers) problem is impossible: handler will send to NATS only if requester is ready to read it
- section is procesed (e.g. sent to http client) too slow -> `ibusnats.ErrSlowConsumer` is returned. I.e. acually `slow consumer` problem is moved from NATS to airs application level. Even if consumer is slow NATS will not be overflowed anymore
- to detect `no consumer` situation need to send something to NATS. I.e. data for one section will be prepared in vain.

# Limitations
- `ibus.IResultSenderClosable`:
  - `sectionType`, `path` elem and `elementName` max length is 255 chars
  - `[]path` max length is 255 elements
- `ibus.IResultSenderClosable.Close()`: failed to send to NATS -> no error, nothing happens, just error is logged. Requester will get `ibus.ErrTimeoutExpired` at `*secErr`
- failed to publish at `ibus.SendResponse()` -> nothing happens, just error is logged. Requester will get `ibus.ErrTimeoutExpired` as `err`
- failed to unsubscribe from the queue on service stop -> nothing happens, just error is logged
- service stops in 10 seconds maximum (message awaiting timeout)
