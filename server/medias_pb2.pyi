from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class AddMediaRequest(_message.Message):
    __slots__ = ["media"]
    MEDIA_FIELD_NUMBER: _ClassVar[int]
    media: Media
    def __init__(self, media: _Optional[_Union[Media, _Mapping]] = ...) -> None: ...

class AddMediaResponse(_message.Message):
    __slots__ = ["count", "message"]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    count: int
    message: str
    def __init__(self, message: _Optional[str] = ..., count: _Optional[int] = ...) -> None: ...

class DeleteMediaRequest(_message.Message):
    __slots__ = ["id"]
    ID_FIELD_NUMBER: _ClassVar[int]
    id: int
    def __init__(self, id: _Optional[int] = ...) -> None: ...

class DeleteMediaResponse(_message.Message):
    __slots__ = ["media"]
    MEDIA_FIELD_NUMBER: _ClassVar[int]
    media: Media
    def __init__(self, media: _Optional[_Union[Media, _Mapping]] = ...) -> None: ...

class GetMediaByIdRequest(_message.Message):
    __slots__ = ["id"]
    ID_FIELD_NUMBER: _ClassVar[int]
    id: int
    def __init__(self, id: _Optional[int] = ...) -> None: ...

class GetMediasRequest(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

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
    __slots__ = ["media"]
    MEDIA_FIELD_NUMBER: _ClassVar[int]
    media: Media
    def __init__(self, media: _Optional[_Union[Media, _Mapping]] = ...) -> None: ...
