# airs-ibusnats
[![codecov](https://codecov.io/gh/untillpro/airs-ibusnats/branch/master/graph/badge.svg?token=HmtGrmC6C1)](https://codecov.io/gh/untillpro/airs-ibusnats)

- NATS implementation of ibus interface
- `Backpressure` is implemented. Next section will not be sent by the service until the sender claimed he is ready to receive it.
- `Slow consumer` problem is eliminated: the client could be as slow as it want. But the service stops communication if the sender processes a section element longer than 10 seconds

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
Misc Stream = GoOn | NoConsumer
GoOn = 0x0
NoConsumer = 0x1
```

# Limitations
- `ibus.IResultSenderClosable`:
  - `sectionType`, `path` elem and `elementName` max length is 255 chars
  - `[]path` max length is 255 elements
- `ibus.IResultSenderClosable.Close()`: failed to send to NATS -> nothing happens, just error is logged. Requester will get `ibus.ErrTimeoutExpired` at `*secErr`
- failed to publish at `ibus.SendResponse()` -> nothing happens, just error is logged. Requester will get `ibus.ErrTimeoutExpired` as `err`
- failed to unsubscribe from the queue on service stop -> nothing happens, just error is logged
- service stops in 10 seconds maximum (message awaiting timeout)
