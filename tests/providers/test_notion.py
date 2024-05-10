from unittest.mock import Mock, patch
import pytest

from knowledge_bridge.models import EdgeEntity, NodeEntity
from knowledge_bridge.providers.notion import (
    NotionProvider,
    parse_datetime,
    process_paginated,
)


@pytest.fixture
def notion_client_mock():
    page1 = {
        "id": "page1",
        "last_edited_time": "2022-01-04T00:00:00Z",
        "created_time": "2022-01-01T00:00:00Z",
        "properties": {},
        "url": "https://example.com/page1",
        "in_trash": False,
        "parent": {
            "type": "workspace",
        },
    }
    page2 = {
        "id": "page2",
        "last_edited_time": "2022-01-05T00:00:00Z",
        "created_time": "2022-01-02T00:00:00Z",
        "properties": {},
        "url": "https://example.com/page2",
        "in_trash": True,
        "parent": {
            "type": "page",
            "page": "page1",
        },
    }
    page3 = {
        "id": "page3",
        "last_edited_time": "2022-01-01T00:00:00Z",
        "created_time": "2022-01-01T00:00:00Z",
        "properties": {},
        "url": "https://example.com/page3",
        "in_trash": True,
        "parent": {
            "type": "database",
            "database": "database1",
        },
    }
    notion_search_page_response = {
        "results": [page1, page2, page3],
        "has_more": False,
    }
    database1 = {
        "id": "database1",
        "last_edited_time": "2022-01-03T00:00:00Z",
        "created_time": "2022-01-01T00:00:00Z",
        "properties": {},
        "url": "https://example.com/database1",
        "in_trash": False,
        "parent": {
            "type": "page",
            "page": "page1",
        },
    }
    database2 = {
        "id": "database2",
        "last_edited_time": "2022-01-04T00:00:00Z",
        "created_time": "2022-01-02T00:00:00Z",
        "properties": {},
        "url": "https://example.com/database2",
        "parent": {"type": "workspace"},
    }
    notion_search_database_response = {
        "results": [database1, database2],
        "has_more": False,
    }
    block1 = {
        "id": "block1",
        "type": "paragraph",
        "paragraph": {"text": [{"type": "text", "text": {"content": "Hello, World!"}}]},
        "last_edited_time": "2022-01-04T00:00:00Z",
        "created_time": "2022-01-02T00:00:00Z",
        "parent": {
            "type": "page",
            "page": "page1",
        },
    }
    block2 = {
        "id": "page2",
        "type": "child_page",
        "last_edited_time": "2022-01-04T00:00:00Z",
        "created_time": "2022-01-02T00:00:00Z",
        "parent": {
            "type": "page",
            "page": "page1",
        },
    }
    block3 = {
        "id": "database1",
        "type": "child_database",
        "last_edited_time": "2022-01-04T00:00:00Z",
        "created_time": "2022-01-02T00:00:00Z",
        "parent": {
            "type": "page",
            "page": "page1",
        },
    }
    notion_list_block_response = {
        "results": [block1, block2, block3],
        "has_more": False,
    }
    notion_databases_query_response = {
        "results": [page3],
        "has_more": False,
    }

    def search_method(query, start_cursor=None, filter=None, **kwargs):
        if filter is None:
            raise ValueError("Filter is required")
        if filter["value"] == "page":
            return notion_search_page_response
        elif filter["value"] == "database":
            return notion_search_database_response
        else:
            raise ValueError("Invalid filter value")

    with patch("knowledge_bridge.providers.notion.Client") as mock:
        mock.return_value.search.side_effect = search_method
        mock.return_value.blocks.children.list.return_value = notion_list_block_response
        mock.return_value.databases.query.return_value = notion_databases_query_response
        mock.return_value.pages.retrieve.return_value = page2
        mock.return_value.databases.retrieve.return_value = database1
        yield mock


@pytest.fixture
def notion_search_endpoint_mock():
    notion_search_endpoint = Mock()
    search_response = [
        {
            "results": [
                {"id": "page1", "last_edited_time": "2022-01-05T00:00:00Z"},
                {"id": "page2", "last_edited_time": "2022-01-04T00:00:00Z"},
            ],
            "has_more": True,
            "next_cursor": "page3",
        },
        {
            "results": [
                {"id": "page3", "last_edited_time": "2022-01-04T00:00:00Z"},
                {"id": "page4", "last_edited_time": "2022-01-03T00:00:00Z"},
            ],
            "has_more": True,
            "next_cursor": "page5",
        },
        {
            "results": [
                {"id": "page5", "last_edited_time": "2022-01-02T00:00:00Z"},
                {"id": "page6", "last_edited_time": "2022-01-01T00:00:00Z"},
            ],
            "has_more": False,
        },
    ]
    notion_search_endpoint.side_effect = search_response
    return notion_search_endpoint


def test_process_paginated(notion_search_endpoint_mock):
    result = list(process_paginated(notion_search_endpoint_mock, query="test"))
    assert len(result) == 6
    assert result == [
        {"id": "page1", "last_edited_time": "2022-01-05T00:00:00Z"},
        {"id": "page2", "last_edited_time": "2022-01-04T00:00:00Z"},
        {"id": "page3", "last_edited_time": "2022-01-04T00:00:00Z"},
        {"id": "page4", "last_edited_time": "2022-01-03T00:00:00Z"},
        {"id": "page5", "last_edited_time": "2022-01-02T00:00:00Z"},
        {"id": "page6", "last_edited_time": "2022-01-01T00:00:00Z"},
    ]
    notion_search_endpoint_mock.assert_any_call(start_cursor=None, query="test")
    notion_search_endpoint_mock.assert_any_call(start_cursor="page3", query="test")
    notion_search_endpoint_mock.assert_any_call(start_cursor="page5", query="test")


def test_process_paginated_last_edited_time(notion_search_endpoint_mock):
    result = list(
        process_paginated(
            notion_search_endpoint_mock,
            last_edited_time=parse_datetime("2022-01-04T00:00:00Z"),
            query="test",
        )
    )
    assert len(result) == 3
    assert result == [
        {"id": "page1", "last_edited_time": "2022-01-05T00:00:00Z"},
        {"id": "page2", "last_edited_time": "2022-01-04T00:00:00Z"},
        {"id": "page3", "last_edited_time": "2022-01-04T00:00:00Z"},
    ]
    notion_search_endpoint_mock.assert_any_call(start_cursor=None, query="test")
    notion_search_endpoint_mock.assert_any_call(start_cursor="page3", query="test")


@pytest.mark.usefixtures("notion_client_mock")
def test_get_latest_data():
    notion_provider = NotionProvider(token="test")
    nodes, edges = notion_provider.get_latest_data(
        last_sync_timestamp=parse_datetime("2022-01-03T00:00:00Z")
    )
    assert nodes == [
        NodeEntity(
            id="page1",
            type="page",
            edited=parse_datetime("2022-01-04T00:00:00Z"),
            created=parse_datetime("2022-01-01T00:00:00Z"),
            text="{}",
            link="https://example.com/page1",
            obsolete=False,
        ),
        NodeEntity(
            id="block1",
            type="block",
            created=parse_datetime("2022-01-02T00:00:00Z"),
            edited=parse_datetime("2022-01-04T00:00:00Z"),
            link=None,
            text='{"text": [{"type": "text", "text": {"content": "Hello, World!"}}]}',
            obsolete=False,
        ),
        NodeEntity(
            id="page2",
            type="page",
            edited=parse_datetime("2022-01-05T00:00:00Z"),
            created=parse_datetime("2022-01-02T00:00:00Z"),
            text="{}",
            link="https://example.com/page2",
            obsolete=True,
        ),
        NodeEntity(
            id="database1",
            type="database",
            edited=parse_datetime("2022-01-03T00:00:00Z"),
            created=parse_datetime("2022-01-01T00:00:00Z"),
            text="{}",
            link=None,
            obsolete=False,
        ),
        NodeEntity(
            id="page3",
            type="page",
            edited=parse_datetime("2022-01-01T00:00:00Z"),
            created=parse_datetime("2022-01-01T00:00:00Z"),
            text="{}",
            link="https://example.com/page3",
            obsolete=True,
        ),
        NodeEntity(
            id="database2",
            type="database",
            edited=parse_datetime("2022-01-04T00:00:00Z"),
            created=parse_datetime("2022-01-02T00:00:00Z"),
            text="{}",
            link=None,
            obsolete=False,
        ),
    ]
    assert sorted(edges, key=lambda x: x.__hash__()) == sorted(
        [
            EdgeEntity(source=nodes[3], target=nodes[4], type="CHILD_PAGE"),
            EdgeEntity(source=nodes[0], target=nodes[2], type="CHILD_PAGE"),
            EdgeEntity(source=nodes[0], target=nodes[1], type="CHILD_BLOCK"),
            EdgeEntity(source=nodes[0], target=nodes[3], type="CHILD_DATABASE"),
        ],
        key=lambda x: x.__hash__(),
    )
