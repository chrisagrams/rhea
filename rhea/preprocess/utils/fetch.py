import requests
import json
from typing import List
import io
import logging
import tarfile

logger = logging.getLogger(__name__)


def get_galaxy_repositories() -> dict[str, List[dict]]:
    url = "https://toolshed.g2.bx.psu.edu/api/repositories"
    logger.info(f"Fetching Galaxy repositories from {url}")

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        logger.info(f"Successfully fetched {len(data)} repositories")
        return data
    except requests.RequestException as e:
        logger.error(f"Failed to fetch Galaxy repositories: {e}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON response: {e}")
        raise


def get_tool_repository_tar(owner: str, name: str) -> io.BytesIO | None:
    repo_url = f"https://toolshed.g2.bx.psu.edu/repos/{owner}/{name}"
    logger.info(f"Downloading repository {owner}/{name} from {repo_url}")

    try:
        # Download the repository archive directly via HTTP
        archive_url = f"{repo_url}/archive/tip.tar.gz"
        logger.debug(f"Downloading archive from {archive_url}")

        response = requests.get(archive_url)
        response.raise_for_status()

        # Return the repository data as BytesIO
        repo_data = io.BytesIO(response.content)
        logger.info(
            f"Successfully downloaded repository {owner}/{name} ({len(response.content)} bytes)"
        )
        return repo_data

    except requests.RequestException as e:
        logger.error(f"Failed to download repository {owner}/{name}: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error downloading repository {owner}/{name}: {e}")
        return None


def cleanup_hg_repo(buffer: io.BytesIO) -> io.BytesIO:
    """
    Remove the .hg/ directory and all of its contents from a tar.gz archive.

    Args:
        buffer: BytesIO containing the original tar.gz archive

    Returns:
        BytesIO containing the cleaned tar.gz archive without .hg/ directory
    """
    logger.info("Starting cleanup of .hg directory from repository archive")

    try:
        buffer.seek(0)

        with tarfile.open(fileobj=buffer, mode="r:gz") as original_tar:
            cleaned_buffer = io.BytesIO()

            with tarfile.open(fileobj=cleaned_buffer, mode="w:gz") as cleaned_tar:
                # Iterate through all members in the original archive
                for member in original_tar.getmembers():
                    # Check if the member is in the .hg directory
                    if (
                        "/.hg/" in member.name
                        or member.name.endswith("/.hg")
                        or member.name == ".hg"
                        or member.name.endswith(".hg_archival.txt")
                        or member.name.startswith(".hg/")
                    ):
                        logger.debug(f"Skipping Mercurial file: {member.name}")
                        continue

                    # For non-.hg files, copy them to the new archive
                    if member.isfile():
                        # Extract file data from original archive
                        file_data = original_tar.extractfile(member)
                        if file_data:
                            # Add the file to the cleaned archive
                            cleaned_tar.addfile(member, file_data)
                    else:
                        # For directories and other types, add them without data
                        cleaned_tar.addfile(member)

                logger.info("Successfully cleaned .hg directory from archive")

            cleaned_buffer.seek(0)
            return cleaned_buffer

    except tarfile.TarError as e:
        logger.error(f"Failed to process tar archive: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during .hg cleanup: {e}")
        raise
