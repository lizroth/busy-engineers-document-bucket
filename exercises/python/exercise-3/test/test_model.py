import uuid
from typing import Dict

import pytest
from document_bucket import config
from document_bucket.model import (DocumentBucketBundle,
                                   DocumentBucketContextItem,
                                   DocumentBucketContextQuery,
                                   DocumentBucketItem,
                                   DocumentBucketItemException,
                                   DocumentBucketPointerItem)


@pytest.fixture
def ctx_prefix() -> str:
    return config["document_bucket"]["document_table"]["ctx_prefix"]


@pytest.fixture
def suuid() -> str:
    return str(uuid.uuid4())


@pytest.fixture
def sample_context() -> Dict[str, str]:
    context = {
        "fleet": "coolbeans",
        "region": "sp-moon-1",
        "usecase": "dev",
        "assertions": "false",
    }
    return context


def test_uuid_item_happy_case(sample_context):
    test_item = DocumentBucketPointerItem.generate(sample_context)
    assert sample_context.items() <= test_item.context.items()


def test_uuid_item_happy_case_from_key_and_context(suuid, sample_context):
    test_item = DocumentBucketPointerItem.from_key_and_context(suuid, sample_context)
    assert suuid in test_item.get_s3_key()
    assert sample_context.items() <= test_item.context.items()


def test_uuid_item_happy_case_str(suuid):
    item = DocumentBucketPointerItem(suuid)
    assert suuid in item.partition_key


def test_invalid_uuid_throws():
    with pytest.raises(ValueError):
        DocumentBucketPointerItem("garbage")


def test_context_item_happy_case(suuid):
    context_key = "FLEET"
    test_item = DocumentBucketContextItem(context_key, suuid)
    assert context_key in test_item.partition_key
    assert suuid in test_item.sort_key


def test_unset_partition_raises_uuid():
    with pytest.raises(DocumentBucketItemException):
        DocumentBucketPointerItem(None, "sort_key")


def test_unset_partition_raises_context_item():
    with pytest.raises(DocumentBucketItemException):
        DocumentBucketContextItem(None, "sort_key")


def test_canonicalize(ctx_prefix):
    test_key = "toTalLy_pUbl1c_7ag"
    c = DocumentBucketContextItem.canonicalize(test_key)
    assert test_key.upper() in c
    assert ctx_prefix.upper() in c


def test_prefix_added_once(ctx_prefix):
    test_key = ctx_prefix.upper() + "C00L"
    assert test_key == DocumentBucketContextItem.canonicalize(test_key)


def test_assert_set():
    item = DocumentBucketItem("foo", "bar")
    item.partition_key = None
    with pytest.raises(DocumentBucketItemException):
        item._assert_set()
    item.partition_key = "foo"
    item.sort_key = None
    with pytest.raises(DocumentBucketItemException):
        item._assert_set()


def test_pointer_item(sample_context):
    data = bytes.fromhex("decafbad")
    bundle = DocumentBucketBundle.from_data_and_context(data, sample_context)
    item = bundle.key.to_item()
    assert sample_context.items() <= item.items()
    assert bundle.key.partition_key in item.values()
    assert bundle.key.get_s3_key() in item.values()


def test_get_s3_key(suuid):
    with pytest.raises(DocumentBucketItemException):
        DocumentBucketItem("foo", "bar").get_s3_key()

    with pytest.raises(DocumentBucketItemException):
        DocumentBucketContextItem("baz", suuid).get_s3_key()

    assert suuid in DocumentBucketPointerItem(suuid).get_s3_key()


def test_context_from_item(sample_context):
    pointer = DocumentBucketPointerItem.generate(sample_context)
    context = pointer.context_from_item(pointer.to_item())
    assert sample_context.items() == context.items()


def test_query_condition():
    query = DocumentBucketContextQuery("fleet")
    assert query.partition_key in query.query_condition()._values


def test_duplicate_keys_throw():
    data = bytes.fromhex("badbadbadbad")
    bonkers_context = {DocumentBucketItem.partition_key_name(): "kaboom"}
    with pytest.raises(DocumentBucketItemException):
        DocumentBucketBundle.from_data_and_context(data, bonkers_context)


def test_validate_reserved_ec_keys_partition():
    with pytest.raises(DocumentBucketItemException):
        DocumentBucketPointerItem._validate_reserved_ec_keys(
            {DocumentBucketItem.partition_key_name(): "blammo"}
        )


def test_validate_reserved_ec_keys_sort():
    with pytest.raises(DocumentBucketItemException):
        DocumentBucketPointerItem._validate_reserved_ec_keys(
            {DocumentBucketItem.sort_key_name(): "blammo"}
        )


def test_setting_pointer_sort_key_throws(suuid, sample_context):
    with pytest.raises(DocumentBucketItemException):
        DocumentBucketPointerItem(suuid, sample_context)


def test_pointer_key_hash(suuid, sample_context):
    pointer1 = DocumentBucketPointerItem(suuid)
    pointer2 = DocumentBucketPointerItem(partition_key=suuid, context=sample_context)
    assert hash(pointer1) == hash(pointer2)
    assert pointer1 == pointer2


def test_object_type_neq(sample_context):
    pointer = DocumentBucketPointerItem.generate(sample_context)
    context = DocumentBucketContextItem("stuff", pointer.get_s3_key())
    assert pointer != context


def test_context_eq(suuid):
    context1 = DocumentBucketContextItem("coolkey", suuid)
    context2 = DocumentBucketContextItem("coolkey", suuid)
    assert context1 == context2
    assert context1 is not context2
