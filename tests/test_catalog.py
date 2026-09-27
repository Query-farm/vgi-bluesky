"""The catalog shape the DuckDB extension will see on ATTACH."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pytest
import yaml

from vgi_bluesky.actors import FollowersFunction, FollowsFunction, ProfileFunction, SearchActorsFunction
from vgi_bluesky.feeds import ActorFeedsFunction, FeedFunction, PopularFeedsFunction
from vgi_bluesky.jetstream import JetstreamFunction
from vgi_bluesky.meta import result_columns_schema
from vgi_bluesky.posts import (
    AuthorFeedFunction,
    LikesFunction,
    PostFunction,
    QuotesFunction,
    RepostedByFunction,
    SearchPostsFunction,
    ThreadFunction,
)
from vgi_bluesky.worker import _BLUESKY_CATALOG, _CATALOG_TAGS, _SCHEMA_TAGS

ROOT = Path(__file__).resolve().parent.parent
SCHEMA = _BLUESKY_CATALOG.schemas[0]
FUNCTIONS = list(SCHEMA.functions)
NAMES = {f.Meta.name for f in FUNCTIONS} | {t.name for t in SCHEMA.tables}

#: Bounded fetches: a batch of profiles, a batch of posts, one thread. These stay
#: blended, so they compose as the inner side of a correlated LATERAL.
BLENDED = [ProfileFunction, PostFunction, ThreadFunction]

#: Cursor-paged endpoints. Stateful scans emit one API page per tick so a LIMIT
#: stops early, which a blended function cannot do.
PAGED_SCANS = [
    SearchActorsFunction,
    FollowersFunction,
    FollowsFunction,
    AuthorFeedFunction,
    SearchPostsFunction,
    LikesFunction,
    RepostedByFunction,
    QuotesFunction,
    FeedFunction,
    PopularFeedsFunction,
    ActorFeedsFunction,
]


class TestRegistration:
    def test_every_function_is_classified(self) -> None:
        """A new function has to be put in one camp or the other deliberately."""
        registered = {f.Meta.name for f in FUNCTIONS} - {"all_trends", "jetstream"}
        assert registered == {f.Meta.name for f in BLENDED + PAGED_SCANS}

    def test_jetstream_is_a_stateful_scan(self) -> None:
        """A stream must emit per tick; a blended function could never return."""
        assert JetstreamFunction.get_metadata().input_from_args is False
        state = JetstreamFunction.initial_state(None)  # type: ignore[arg-type]
        assert state.scan_id and state.cursor == 0 and not state.done

    @pytest.mark.parametrize("func", BLENDED, ids=lambda f: f.Meta.name)
    def test_blended_positional_args_are_input_columns(self, func: type) -> None:
        assert func.get_metadata().input_from_args is True  # type: ignore[attr-defined]

    @pytest.mark.parametrize("func", BLENDED, ids=lambda f: f.Meta.name)
    def test_blended_has_no_finalize(self, func: type) -> None:
        """DuckDB rejects LATERAL on a table function registering a finalize callback."""
        assert func.has_finalize_override() is False  # type: ignore[attr-defined]

    @pytest.mark.parametrize("func", PAGED_SCANS, ids=lambda f: f.Meta.name)
    def test_paged_scans_are_not_blended(self, func: type) -> None:
        assert func.get_metadata().input_from_args is False  # type: ignore[attr-defined]

    @pytest.mark.parametrize("func", PAGED_SCANS, ids=lambda f: f.Meta.name)
    def test_paged_scans_start_from_a_fresh_cursor(self, func: type) -> None:
        state = func.initial_state(None)  # type: ignore[attr-defined]
        assert state.cursor == "" and state.done is False

    def test_the_trends_table_is_backed_by_all_trends(self) -> None:
        (table,) = SCHEMA.tables
        assert table.name == "trends"
        assert table.function is not None and table.function.Meta.name == "all_trends"


class TestDocumentation:
    @pytest.mark.parametrize("func", FUNCTIONS, ids=lambda f: f.Meta.name)
    def test_declared_result_columns_match_the_schema(self, func: type) -> None:
        """The tag DuckDB shows before bind must be the schema the function actually returns."""
        declared = func.Meta.tags["vgi.result_columns_schema"]  # type: ignore[attr-defined]
        assert declared == result_columns_schema(func.FIXED_SCHEMA)  # type: ignore[attr-defined]

    @pytest.mark.parametrize("func", FUNCTIONS, ids=lambda f: f.Meta.name)
    def test_docs_and_examples_are_present(self, func: type) -> None:
        tags = func.Meta.tags  # type: ignore[attr-defined]
        assert tags["vgi.doc_llm"] and tags["vgi.doc_md"]
        assert json.loads(tags["vgi.example_queries"])
        assert func.Meta.examples  # type: ignore[attr-defined]

    def test_categories_used_are_declared(self) -> None:
        declared = {c["name"] for c in json.loads(_SCHEMA_TAGS["vgi.categories"])}
        used = {f.Meta.tags["vgi.category"] for f in FUNCTIONS} | {  # type: ignore[attr-defined]
            t.tags["vgi.category"] for t in SCHEMA.tables
        }
        assert used <= declared, used - declared


def _graders() -> list[dict[str, Any]]:
    return list(yaml.safe_load((ROOT / "vgi-agent-tests.yaml").read_text())["tasks"])


def _grader_sql(task: dict[str, Any]) -> list[str]:
    ref = task.get("reference_sql") or []
    return [
        *(ref if isinstance(ref, list) else [ref]),
        *([task["check_sql"]] if task.get("check_sql") else []),
    ]


class TestAgentTasks:
    """The published tasks are prompts only; the graders live in a private sidecar."""

    PUBLIC = json.loads(_CATALOG_TAGS["vgi.agent_test_tasks"])

    def test_published_tasks_carry_no_answers(self) -> None:
        assert all(set(task) == {"name", "prompt"} for task in self.PUBLIC)

    def test_every_task_has_exactly_one_grader(self) -> None:
        graders = [g["name"] for g in _graders()]
        assert sorted(graders) == sorted(t["name"] for t in self.PUBLIC)

    def test_every_object_is_exercised_by_some_task(self) -> None:
        """Mirrors vgi-lint VGI520, so a new function without a task fails here first."""
        referenced = {
            name
            for g in _graders()
            for sql in _grader_sql(g)
            for name in re.findall(r"bluesky\.main\.([a-z_]+)", sql)
        }
        assert referenced >= NAMES, NAMES - referenced


def _all_example_sql() -> list[str]:
    sql = [e["sql"] for e in json.loads(_SCHEMA_TAGS["vgi.example_queries"])]
    sql += [e["sql"] for e in json.loads(_CATALOG_TAGS["vgi.executable_examples"])]
    for grader in _graders():
        sql += _grader_sql(grader)
    for f in FUNCTIONS:
        sql += [e["sql"] for e in json.loads(f.Meta.tags["vgi.example_queries"])]  # type: ignore[attr-defined]
        sql += [e.sql for e in f.Meta.examples]  # type: ignore[attr-defined]
    for t in SCHEMA.tables:
        sql += [e["sql"] for e in json.loads(t.tags["vgi.example_queries"])]
    return sql


class TestExamplesReferenceRealObjects:
    """An example naming a function that does not exist is documentation that lies."""

    @pytest.mark.parametrize("sql", _all_example_sql())
    def test_every_qualified_name_exists(self, sql: str) -> None:
        referenced = set(re.findall(r"bluesky\.main\.([a-z_]+)", sql))
        assert referenced, f"example does not reference the catalog: {sql}"
        assert referenced <= NAMES, referenced - NAMES
