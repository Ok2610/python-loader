from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class AddMediaRequest(_message.Message):
    __slots__ = ["media"]
    MEDIA_FIELD_NUMBER: _ClassVar[int]
    media: Media
    def __init__(self, media: _Optional[_Union[Media, _Mapping]] = ...) -> None: ...

class AddMediaStreamResponse(_message.Message):
    __slots__ = ["count", "success"]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    count: int
    success: bool
    def __init__(self, success: bool = ..., count: _Optional[int] = ...) -> None: ...

class AlphanumericalValue(_message.Message):
    __slots__ = ["value"]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: str
    def __init__(self, value: _Optional[str] = ...) -> None: ...

class CreateHierarchyRequest(_message.Message):
    __slots__ = ["name", "rootNodeId", "tagsetId"]
    NAME_FIELD_NUMBER: _ClassVar[int]
    ROOTNODEID_FIELD_NUMBER: _ClassVar[int]
    TAGSETID_FIELD_NUMBER: _ClassVar[int]
    name: str
    rootNodeId: int
    tagsetId: int
    def __init__(self, name: _Optional[str] = ..., tagsetId: _Optional[int] = ..., rootNodeId: _Optional[int] = ...) -> None: ...

class CreateNodeRequest(_message.Message):
    __slots__ = ["hierarchyId", "parentNodeId", "tagId"]
    HIERARCHYID_FIELD_NUMBER: _ClassVar[int]
    PARENTNODEID_FIELD_NUMBER: _ClassVar[int]
    TAGID_FIELD_NUMBER: _ClassVar[int]
    hierarchyId: int
    parentNodeId: int
    tagId: int
    def __init__(self, tagId: _Optional[int] = ..., hierarchyId: _Optional[int] = ..., parentNodeId: _Optional[int] = ...) -> None: ...

class CreateTagRequest(_message.Message):
    __slots__ = ["alphanumerical", "date", "numerical", "tagSetId", "tagTypeId", "time", "timestamp"]
    ALPHANUMERICAL_FIELD_NUMBER: _ClassVar[int]
    DATE_FIELD_NUMBER: _ClassVar[int]
    NUMERICAL_FIELD_NUMBER: _ClassVar[int]
    TAGSETID_FIELD_NUMBER: _ClassVar[int]
    TAGTYPEID_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    TIME_FIELD_NUMBER: _ClassVar[int]
    alphanumerical: AlphanumericalValue
    date: DateValue
    numerical: NumericalValue
    tagSetId: int
    tagTypeId: int
    time: TimeValue
    timestamp: TimeStampValue
    def __init__(self, tagSetId: _Optional[int] = ..., tagTypeId: _Optional[int] = ..., alphanumerical: _Optional[_Union[AlphanumericalValue, _Mapping]] = ..., timestamp: _Optional[_Union[TimeStampValue, _Mapping]] = ..., time: _Optional[_Union[TimeValue, _Mapping]] = ..., date: _Optional[_Union[DateValue, _Mapping]] = ..., numerical: _Optional[_Union[NumericalValue, _Mapping]] = ...) -> None: ...

class CreateTagSetRequest(_message.Message):
    __slots__ = ["name", "tagTypeId"]
    NAME_FIELD_NUMBER: _ClassVar[int]
    TAGTYPEID_FIELD_NUMBER: _ClassVar[int]
    name: str
    tagTypeId: int
    def __init__(self, name: _Optional[str] = ..., tagTypeId: _Optional[int] = ...) -> None: ...

class CreateTaggingRequest(_message.Message):
    __slots__ = ["mediaId", "tagId"]
    MEDIAID_FIELD_NUMBER: _ClassVar[int]
    TAGID_FIELD_NUMBER: _ClassVar[int]
    mediaId: int
    tagId: int
    def __init__(self, mediaId: _Optional[int] = ..., tagId: _Optional[int] = ...) -> None: ...

class DateValue(_message.Message):
    __slots__ = ["value"]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: str
    def __init__(self, value: _Optional[str] = ...) -> None: ...

class EmptyRequest(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class GetHierarchyRequest(_message.Message):
    __slots__ = ["tagId"]
    TAGID_FIELD_NUMBER: _ClassVar[int]
    tagId: int
    def __init__(self, tagId: _Optional[int] = ...) -> None: ...

class GetMediaIdFromURIRequest(_message.Message):
    __slots__ = ["uri"]
    URI_FIELD_NUMBER: _ClassVar[int]
    uri: str
    def __init__(self, uri: _Optional[str] = ...) -> None: ...

class GetTagSetRequestByName(_message.Message):
    __slots__ = ["name"]
    NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    def __init__(self, name: _Optional[str] = ...) -> None: ...

class Hierachy(_message.Message):
    __slots__ = ["id", "name", "rootNodeId", "tagsetId"]
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    ROOTNODEID_FIELD_NUMBER: _ClassVar[int]
    TAGSETID_FIELD_NUMBER: _ClassVar[int]
    id: int
    name: str
    rootNodeId: int
    tagsetId: int
    def __init__(self, id: _Optional[int] = ..., name: _Optional[str] = ..., tagsetId: _Optional[int] = ..., rootNodeId: _Optional[int] = ...) -> None: ...

class HierarchyResponse(_message.Message):
    __slots__ = ["Hierachy", "success"]
    HIERACHY_FIELD_NUMBER: _ClassVar[int]
    Hierachy: Hierachy
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    success: bool
    def __init__(self, success: bool = ..., Hierachy: _Optional[_Union[Hierachy, _Mapping]] = ...) -> None: ...

class IdRequest(_message.Message):
    __slots__ = ["id"]
    ID_FIELD_NUMBER: _ClassVar[int]
    id: int
    def __init__(self, id: _Optional[int] = ...) -> None: ...

class IdResponse(_message.Message):
    __slots__ = ["id", "success"]
    ID_FIELD_NUMBER: _ClassVar[int]
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    id: int
    success: bool
    def __init__(self, success: bool = ..., id: _Optional[int] = ...) -> None: ...

class Media(_message.Message):
    __slots__ = ["file_type", "file_uri", "id", "thumbnail_uri"]
    FILE_TYPE_FIELD_NUMBER: _ClassVar[int]
    FILE_URI_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    THUMBNAIL_URI_FIELD_NUMBER: _ClassVar[int]
    file_type: int
    file_uri: str
    id: int
    thumbnail_uri: str
    def __init__(self, id: _Optional[int] = ..., file_uri: _Optional[str] = ..., file_type: _Optional[int] = ..., thumbnail_uri: _Optional[str] = ...) -> None: ...

class MediaResponse(_message.Message):
    __slots__ = ["media", "success"]
    MEDIA_FIELD_NUMBER: _ClassVar[int]
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    media: Media
    success: bool
    def __init__(self, success: bool = ..., media: _Optional[_Union[Media, _Mapping]] = ...) -> None: ...

class Node(_message.Message):
    __slots__ = ["hierarchyId", "id", "parentNodeId", "tagId"]
    HIERARCHYID_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    PARENTNODEID_FIELD_NUMBER: _ClassVar[int]
    TAGID_FIELD_NUMBER: _ClassVar[int]
    hierarchyId: int
    id: int
    parentNodeId: int
    tagId: int
    def __init__(self, id: _Optional[int] = ..., tagId: _Optional[int] = ..., hierarchyId: _Optional[int] = ..., parentNodeId: _Optional[int] = ...) -> None: ...

class NodeResponse(_message.Message):
    __slots__ = ["success"]
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    success: bool
    def __init__(self, success: bool = ...) -> None: ...

class NumericalValue(_message.Message):
    __slots__ = ["value"]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: int
    def __init__(self, value: _Optional[int] = ...) -> None: ...

class StatusResponse(_message.Message):
    __slots__ = ["success"]
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    success: bool
    def __init__(self, success: bool = ...) -> None: ...

class Tag(_message.Message):
    __slots__ = ["alphanumerical", "date", "id", "numerical", "tagSetId", "tagTypeId", "time", "timestamp"]
    ALPHANUMERICAL_FIELD_NUMBER: _ClassVar[int]
    DATE_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    NUMERICAL_FIELD_NUMBER: _ClassVar[int]
    TAGSETID_FIELD_NUMBER: _ClassVar[int]
    TAGTYPEID_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    TIME_FIELD_NUMBER: _ClassVar[int]
    alphanumerical: AlphanumericalValue
    date: DateValue
    id: int
    numerical: NumericalValue
    tagSetId: int
    tagTypeId: int
    time: TimeValue
    timestamp: TimeStampValue
    def __init__(self, id: _Optional[int] = ..., tagSetId: _Optional[int] = ..., tagTypeId: _Optional[int] = ..., alphanumerical: _Optional[_Union[AlphanumericalValue, _Mapping]] = ..., timestamp: _Optional[_Union[TimeStampValue, _Mapping]] = ..., time: _Optional[_Union[TimeValue, _Mapping]] = ..., date: _Optional[_Union[DateValue, _Mapping]] = ..., numerical: _Optional[_Union[NumericalValue, _Mapping]] = ...) -> None: ...

class TagResponse(_message.Message):
    __slots__ = ["success", "tag"]
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    TAG_FIELD_NUMBER: _ClassVar[int]
    success: bool
    tag: Tag
    def __init__(self, success: bool = ..., tag: _Optional[_Union[Tag, _Mapping]] = ...) -> None: ...

class TagSet(_message.Message):
    __slots__ = ["id", "name", "tagTypeId"]
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    TAGTYPEID_FIELD_NUMBER: _ClassVar[int]
    id: int
    name: str
    tagTypeId: int
    def __init__(self, id: _Optional[int] = ..., name: _Optional[str] = ..., tagTypeId: _Optional[int] = ...) -> None: ...

class TagSetResponse(_message.Message):
    __slots__ = ["success", "tagset"]
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    TAGSET_FIELD_NUMBER: _ClassVar[int]
    success: bool
    tagset: TagSet
    def __init__(self, success: bool = ..., tagset: _Optional[_Union[TagSet, _Mapping]] = ...) -> None: ...

class Tagging(_message.Message):
    __slots__ = ["mediaId", "tagId"]
    MEDIAID_FIELD_NUMBER: _ClassVar[int]
    TAGID_FIELD_NUMBER: _ClassVar[int]
    mediaId: int
    tagId: int
    def __init__(self, mediaId: _Optional[int] = ..., tagId: _Optional[int] = ...) -> None: ...

class TaggingResponse(_message.Message):
    __slots__ = ["success", "tagging"]
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    TAGGING_FIELD_NUMBER: _ClassVar[int]
    success: bool
    tagging: Tagging
    def __init__(self, success: bool = ..., tagging: _Optional[_Union[Tagging, _Mapping]] = ...) -> None: ...

class TimeStampValue(_message.Message):
    __slots__ = ["value"]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: str
    def __init__(self, value: _Optional[str] = ...) -> None: ...

class TimeValue(_message.Message):
    __slots__ = ["value"]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: str
    def __init__(self, value: _Optional[str] = ...) -> None: ...
