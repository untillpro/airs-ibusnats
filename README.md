# airs-ibusnats
[![codecov](https://codecov.io/gh/untillpro/airs-ibusnats/branch/master/graph/badge.svg?token=HmtGrmC6C1)](https://codecov.io/gh/untillpro/airs-ibusnats)

NATS implementation of ibus interface

# Bus packets binary formats
```
Response Stream = Response | {MapSection+Elem {Elem} | ArraySection+Elem {Elem} | ObjectSection+Elem} Close
Reponse = 0x0 ContentTypeLen_1 ContentType StatusCode_2 Data
MapSection+Elem = 0x2 PathsCount_1 {PathLen_1 Path} LenSectionType_1 SectionType LenElemName_1 ElemName ElemBytes
ArraySection+Elem = 0x3 PathsCount_1 {PathLen_1 Path} LenSectionType_1 SectionType ElemBytes
ObjectSection+Elem = 0x4 PathsCount_1 {PathLen_1 Path} LenSectionType_1 SectionType ElemBytes
Elem = 0x5 [LenElemName_1 ElemName] ElemBytes
Close = 0x1 [ErrorMessage]
```

# Limitations
- `ibus.IResultSenderClosable`:
  - `sectionType`, `path` elem and `elementName` max length is 255 chars
  - `[]path` max length is 255 elements
- `ibus.IResultSenderClosable.Close()`: failed to send to NATS -> no error, nothing happens, just error is logged. Requester will get `ibus.ErrTimeoutExpired` at `*secErr`
- failed to publish at `ibus.SendResponse()` -> nothing happens, just error is logged. Requester will get `ibus.ErrTimeoutExpired` as `err`
- failed to unsubscribe from the queue on service stop -> nothing happens, just error is logged
- Service stops in 10 seconds maximum (message awaiting timeout)
