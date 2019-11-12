import copy
import uuid
from dataclasses import dataclass, field
from typing import Dict, Optional, Set, Union
from uuid import UUID

from boto3.dynamodb.conditions import Key  # type: ignore

from . import config


class DocumentBucketItemException(Exception):
    pass


# FIXME seebees rightly points out that everything bieng named DocumentBucket makes
# the name useless; rename
@dataclass
class DocumentBucketItem:
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
            raise DocumentBucketItemException(
                "partition_key not set correctly after init!"
            )
        if self.sort_key is None:
            raise DocumentBucketItemException("sort_key not set correctly after init!")

    def get_s3_key(self) -> str:
        raise DocumentBucketItemException(
            "Cannot use a {} as an S3 Key!".format(self.__class__)
        )

    def to_key(self):
        key = {
            DocumentBucketItem.partition_key_name(): self.partition_key,
            DocumentBucketItem.sort_key_name(): self.sort_key,
        }
        return key


@dataclass
class DocumentBucketContextQuery:
    partition_key: str

    def __post_init__(self):
        self.partition_key = DocumentBucketContextItem.canonicalize(self.partition_key)

    def query_condition(self):
        return Key(DocumentBucketItem.partition_key_name()).eq(self.partition_key)


@dataclass
class DocumentBucketContextItem(DocumentBucketItem):
    _prefix: str = config["document_bucket"]["document_table"]["ctx_prefix"].upper()

    def __hash__(self):
        return super().__hash__()

    def __eq__(self, other):
        return super().__eq__(other)

    @staticmethod
    def is_context_key_fmt(key: str) -> bool:
        return key.startswith(DocumentBucketContextItem._prefix)

    @staticmethod
    def canonicalize(context_key: str) -> str:
        context_key = context_key.upper()
        if not DocumentBucketContextItem.is_context_key_fmt(context_key):
            context_key = DocumentBucketContextItem._prefix + context_key
        return context_key

    def __post_init__(self):
        self._assert_set()
        self.partition_key = DocumentBucketContextItem.canonicalize(self.partition_key)
        self.sort_key = str(UUID(self.sort_key))


@dataclass
class DocumentBucketPointerItem(DocumentBucketItem):
    sort_key: str = config["document_bucket"]["document_table"]["object_target"]
    context: Dict[str, str] = field(default_factory=dict)

    def __hash__(self):
        # Stick to the partition and sort key as the unique identifier of the record
        return super().__hash__()

    def __eq__(self, other):
        return super().__eq__(other)

    @staticmethod
    def _generate_uuid() -> UUID:
        return uuid.uuid4()

    @staticmethod
    def generate(context: Dict[str, str]):
        return DocumentBucketPointerItem(
            partition_key=DocumentBucketPointerItem._generate_uuid(), context=context
        )

    @staticmethod
    def from_key_and_context(key: str, context: Dict[str, str]):
        return DocumentBucketPointerItem(partition_key=key, context=context)

    @staticmethod
    def _validate_reserved_ec_keys(context: Dict[str, str]):
        pkn = DocumentBucketItem.partition_key_name()
        skn = DocumentBucketItem.sort_key_name()
        if pkn in context.keys() or skn in context.keys():
            raise DocumentBucketItemException(
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
        DocumentBucketPointerItem._validate_reserved_ec_keys(self.context)
        expected_sort_key: str = config["document_bucket"]["document_table"][
            "object_target"
        ]
        if self.sort_key != expected_sort_key:
            raise DocumentBucketItemException(
                "Sort key should be {}, was {}".format(expected_sort_key, self.sort_key)
            )
        self.partition_key = str(self.partition_key)

    def context_from_item(self, item: Dict[str, str]) -> Dict[str, str]:
        del item[DocumentBucketItem.partition_key_name()]
        del item[DocumentBucketItem.sort_key_name()]
        return copy.deepcopy(item)

    def context_items(self) -> Set[DocumentBucketContextItem]:
        result: Set[DocumentBucketContextItem] = set()
        for context_key in self.context.keys():
            result.add(DocumentBucketContextItem(context_key, self.get_s3_key()))
        return result

    def to_item(self):
        key = self.to_key()
        item = {**key, **self.context}
        return item


@dataclass
class DocumentBucketBundle:
    key: DocumentBucketItem
    data: bytes

    @staticmethod
    def from_data_and_context(data: bytes, context: Dict[str, str]):
        key = DocumentBucketPointerItem.generate(context)
        return DocumentBucketBundle(key, data)
