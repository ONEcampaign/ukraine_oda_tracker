from pathlib import Path


class PATHS:
    """Class to store the paths to the data and output folders."""

    project = Path(__file__).resolve().parent.parent
    scripts = project / "scripts"
    raw_data = project / "raw_data"
    output = project / "output"
    pydeflate = raw_data / ".pydeflate"


# -----------------------------------------------------------------------------

ARTICLE_COUNT: int = 50

DT_BASE: str = (
    "https://cms.donortracker.org/items/policy_updates?fields=title&fields=slug&"
    "fields=publish_date&fields=content&fields=sources&fields="
    "funders.funder_profiles_id.name&fields=topics.topics_id.name"
    "&filter={%22status%22:%22published%22}&sort=-publish_date"
    f"&limit={ARTICLE_COUNT}&page=1&meta=filter_count&search="
)

DT_SEARCH: str = "ukraine"
