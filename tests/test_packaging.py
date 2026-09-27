"""The two entry-point scripts must be runnable on their own.

`uv run bluesky_worker.py` resolves dependencies from the script's PEP-723
header, not from `pyproject.toml`, so the two lists drift silently: the package
imports fine under `pytest` while the worker a client actually launches dies at
import. That happened in vgi-kalshi, which this worker is modelled on, and
nothing but an end-to-end ATTACH noticed.
"""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

import httpx
import pytest

ROOT = Path(__file__).resolve().parent.parent
SCRIPTS = ("bluesky_worker.py", "serve.py")

#: PEP 723 inline script metadata: a `# /// script` ... `# ///` comment block.
_BLOCK = re.compile(r"^# /// script$(.+?)^# ///$", re.MULTILINE | re.DOTALL)


def _script_dependencies(path: Path) -> set[str]:
    """The distribution names a script's inline metadata declares."""
    match = _BLOCK.search(path.read_text())
    assert match is not None, f"{path.name} has no PEP-723 script header"
    body = "".join(
        line.removeprefix("# ").removeprefix("#") for line in match.group(1).splitlines(keepends=True)
    )
    return {_name(spec) for spec in tomllib.loads(body)["dependencies"]}


def _name(requirement: str) -> str:
    """The bare distribution name from a requirement string."""
    return re.split(r"[\[><=!~;\s]", requirement, maxsplit=1)[0].strip().lower()


@pytest.fixture(scope="module")
def project() -> dict:
    return tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]


@pytest.fixture(scope="module")
def project_dependencies() -> set[str]:
    data = tomllib.loads((ROOT / "pyproject.toml").read_text())
    return {_name(spec) for spec in data["project"]["dependencies"]}


class TestScriptHeaders:
    @pytest.mark.parametrize("script", SCRIPTS)
    def test_header_covers_every_runtime_dependency(
        self, script: str, project_dependencies: set[str]
    ) -> None:
        declared = _script_dependencies(ROOT / script)
        missing = project_dependencies - declared
        assert missing == set(), (
            f"{script} would fail at import: its PEP-723 header is missing {sorted(missing)}"
        )

    @pytest.mark.parametrize("script", SCRIPTS)
    def test_header_declares_nothing_unknown(self, script: str, project_dependencies: set[str]) -> None:
        """The scripts also pull vgi-rpc directly, but nothing beyond that."""
        extra = _script_dependencies(ROOT / script) - project_dependencies - {"vgi-rpc"}
        assert extra == set(), f"{script} declares dependencies the project does not: {sorted(extra)}"


def _vgi_python_extras(requirements: list[str]) -> set[str]:
    """The extras requested on the vgi-python requirement."""
    (spec,) = [r for r in requirements if _name(r) == "vgi-python"]
    match = re.search(r"\[([^\]]*)\]", spec)
    return {e.strip() for e in match.group(1).split(",")} if match else set()


def _script_requirements(path: Path) -> list[str]:
    match = _BLOCK.search(path.read_text())
    assert match is not None
    body = "".join(
        line.removeprefix("# ").removeprefix("#") for line in match.group(1).splitlines(keepends=True)
    )
    return list(tomllib.loads(body)["dependencies"])


class TestFilterEngine:
    """vgi-python binds a pushed-down WHERE with an in-process DuckDB engine.

    Without the `haybarn` extra the worker imports and binds fine, and then every
    filtered scan fails at runtime with "No DuckDB-compatible engine is
    installed". Only a real ATTACH noticed; this test notices first.
    """

    def test_pyproject_requests_the_engine(self, project: dict) -> None:
        assert "haybarn" in _vgi_python_extras(project["dependencies"])

    @pytest.mark.parametrize("script", SCRIPTS)
    def test_scripts_request_the_engine(self, script: str) -> None:
        assert "haybarn" in _vgi_python_extras(_script_requirements(ROOT / script))


class TestTransport:
    """Responses are compressed on the wire.

    Bluesky's AppView answers gzip and ignores Brotli (offered only `br`, it
    sends the body uncompressed — measured, 698 KB against 88 KB gzipped for
    one page of an author feed), so no Brotli dependency is carried. httpx
    offers gzip on its own; this checks nothing has turned it off.
    """

    def test_gzip_is_offered(self) -> None:
        seen: dict[str, httpx.Headers] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            seen["headers"] = request.headers
            return httpx.Response(200, json={})

        from vgi_bluesky import bluesky_api

        with httpx.Client(transport=httpx.MockTransport(handler), timeout=bluesky_api.TIMEOUT) as client:
            client.get("https://example.invalid/x")
        assert "gzip" in seen["headers"]["accept-encoding"]


class TestLicenseMetadata:
    """The MIT declaration has to survive in the two forms tools actually read.

    Both went wrong at once in vgi-kalshi: GitHub classified the repository as
    license "Other" because a trailing paragraph in LICENSE pushed the file past
    what licensee will match, and the built metadata carried a PEP 639
    `License-Expression` *and* a deprecated license classifier, which is the
    combination `twine` rejects.
    """

    def test_license_is_an_spdx_expression(self, project: dict) -> None:
        assert project["license"] == "MIT"

    def test_no_deprecated_license_classifier(self, project: dict) -> None:
        """PEP 639: license classifiers are deprecated once an expression is set."""
        offenders = [c for c in project.get("classifiers", []) if c.startswith("License ::")]
        assert offenders == [], (
            f'remove {offenders} — `license = "MIT"` already emits License-Expression, '
            "and carrying both is what packaging tools reject"
        )

    def test_license_file_is_bare_mit(self) -> None:
        """Anything appended after the MIT text breaks GitHub's detection.

        The Bluesky content-terms carve-out lives in NOTICE for exactly this reason.
        """
        text = (ROOT / "LICENSE").read_text()
        assert text.startswith("MIT License")
        assert "Query Farm LLC" in text
        assert text.rstrip().endswith("SOFTWARE.")

    def test_carve_out_survives_in_notice(self) -> None:
        """Moving it out of LICENSE must not have dropped it."""
        notice = (ROOT / "NOTICE").read_text()
        assert "bsky.social/about/support/tos" in notice
        assert "MIT License" in notice
