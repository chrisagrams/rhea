import pytest

import io
from rhea.preprocess.utils.fetch import *


@pytest.fixture
def repos():
    return get_galaxy_repositories()


@pytest.fixture
def tool_repo(repos):
    return repos[0]


def test_get_repos():
    repos = get_galaxy_repositories()
    assert isinstance(repos, list)
    assert len(repos) > 0


def test_get_tool_repo_tar(tool_repo):
    buffer: io.BytesIO | None = get_tool_repository_tar(
        tool_repo["owner"], tool_repo["name"]
    )
    assert buffer is not None
    assert len(buffer.getbuffer()) > 0


def test_remove_hg(tool_repo):
    buffer: io.BytesIO | None = get_tool_repository_tar(
        tool_repo["owner"], tool_repo["name"]
    )
    assert buffer is not None

    cleaned_buffer: io.BytesIO | None = cleanup_hg_repo(buffer)
    assert cleaned_buffer is not None

    assert len(cleaned_buffer.getbuffer()) < len(buffer.getbuffer())
