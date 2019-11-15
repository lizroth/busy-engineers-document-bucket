import copy
import uuid
from dataclasses import dataclass, field
from typing import Dict, Optional, Set, Union
from uuid import UUID

from boto3.dynamodb.conditions import Key  # type: ignore

from .config import config


class DataModelException(Exception):
    pass


@dataclass
class BaseItem:
    partition_key: Union[UUID, str]
    sort_key: Optional[Union[str, UUID]]

    def __hash__(self):
        return hash((self.partition_key, self.sort_key))

    def __eq__(self, other):
        if self.__class__ == other.__class__:
            same_keys = other.partition_key == self.partition_key
            same_keys = same_keys and other.sort_key == self.sort_key
            return same_keys
        return False

    @classmethod
    def partition_key_name(cls) -> str:
        return config["document_bucket"]["document_table"]["partition_key"]

    @classmethod
    def sort_key_name(cls) -> str:
        return config["document_bucket"]["document_table"]["sort_key"]

    def _assert_set(self):
        if self.partition_key is None:
            raise DataModelException("partition_key not set correctly after init!")
        if self.sort_key is None:
            raise DataModelException("sort_key not set correctly after init!")

    def get_s3_key(self) -> str:
        raise DataModelException("Cannot use a {} as an S3 Key!".format(self.__class__))

    def to_key(self):
        key = {
            BaseItem.partition_key_name(): self.partition_key,
            BaseItem.sort_key_name(): self.sort_key,
        }
        return key


@dataclass
class ContextQuery:
    partition_key: str

    def __post_init__(self):
        self.partition_key = ContextItem.canonicalize(self.partition_key)

    def expression(self) -> Dict[str, str]:
        return Key(BaseItem.partition_key_name()).eq(self.partition_key)


@dataclass
class ContextItem(BaseItem):
    def __hash__(self):
        return super().__hash__()

    def __eq__(self, other):
        return super().__eq__(other)

    @classmethod
    def _prefix(cls) -> str:
        return config["document_bucket"]["document_table"]["ctx_prefix"].upper()

    @classmethod
    def is_context_key_fmt(cls, key: str) -> bool:
        return key.startswith(ContextItem._prefix())

    @classmethod
    def canonicalize(cls, context_key: str) -> str:
        context_key = context_key.upper()
        if not ContextItem.is_context_key_fmt(context_key):
            context_key = ContextItem._prefix() + context_key
        return context_key

    def __post_init__(self):
        self._assert_set()
        self.partition_key = ContextItem.canonicalize(self.partition_key)
        self.sort_key = str(UUID(self.sort_key))


@dataclass
class PointerItem(BaseItem):
    sort_key: str = config["document_bucket"]["document_table"]["object_target"]
    context: Dict[str, str] = field(default_factory=dict)

    def __hash__(self):
        # Stick to the partition and sort key as the unique identifier of the record
        return super().__hash__()

    def __eq__(self, other):
        return super().__eq__(other)

    @classmethod
    def sort_key_config(cls) -> str:
        return config["document_bucket"]["document_table"]["object_target"]

    @classmethod
    def _generate_uuid(cls) -> UUID:
        return uuid.uuid4()

    @classmethod
    def generate(cls, context: Dict[str, str]):
        return PointerItem(partition_key=cls._generate_uuid(), context=context)

    @classmethod
    def from_key_and_context(cls, key: str, context: Dict[str, str]):
        return cls(partition_key=key, context=context)

    @staticmethod
    def _validate_reserved_ec_keys(context: Dict[str, str]):
        pkn = BaseItem.partition_key_name()
        skn = BaseItem.sort_key_name()
        if pkn in context.keys() or skn in context.keys():
            raise DataModelException(
                "Can't use DB key names ({}, {}) as Encryption Context keys!".format(
                    pkn, skn
                )
            )

    def get_s3_key(self) -> str:
        return str(self.partition_key)

    def __post_init__(self):
        self._assert_set()
        if isinstance(self.partition_key, str):
            # Validate that the UUID is well formed before continuing.
            self.partition_key = str(UUID(self.partition_key))
        PointerItem._validate_reserved_ec_keys(self.context)
        if self.sort_key != self.sort_key_config():
            raise DataModelException(
                "Sort key should be {}, was {}".format(
                    self.sort_key_config(), self.sort_key
                )
            )
        self.partition_key = str(self.partition_key)

    def context_from_item(self, item: Dict[str, str]) -> Dict[str, str]:
        if item is None:
            raise DataModelException("Got empty pointer item!")
        del item[BaseItem.partition_key_name()]
        del item[BaseItem.sort_key_name()]
        return copy.deepcopy(item)

    def context_items(self) -> Set[ContextItem]:
        result: Set[ContextItem] = set()
        for context_key in self.context.keys():
            result.add(ContextItem(context_key, self.get_s3_key()))
        return result

    @classmethod
    def filter_for(cls):
        return Key(BaseItem.sort_key_name()).eq(cls.sort_key)

    def to_item(self):
        key = self.to_key()
        item = {**key, **self.context}
        return item


@dataclass
class DocumentBundle:
    key: BaseItem
    data: bytes

    @staticmethod
    def from_data_and_context(data: bytes, context: Dict[str, str]):
        key = PointerItem.generate(context)
        return DocumentBundle(key, data)
