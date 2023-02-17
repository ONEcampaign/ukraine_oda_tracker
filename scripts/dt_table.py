import json

import pandas as pd
import requests

from scripts import config

ARTICLE_COUNT: int = 50

DT_BASE: str = (
    "https://cms.donortracker.org/items/policy_updates?fields=title&fields=slug&"
    "fields=publish_date&fields=content&fields=sources&fields="
    "funders.funder_profiles_id.name&fields=topics.topics_id.name"
    "&filter={%22status%22:%22published%22}&sort=-publish_date"
    f"&limit={ARTICLE_COUNT}&page=1&meta=filter_count&search="
)

DT_SEARCH: str = "ukraine"


def download_dt_data() -> json:
    """Get data from Donor Tracker"""
    url = DT_BASE + DT_SEARCH
    r = requests.get(url)
    return r.json()


def update_dt_data() -> None:
    """Update Donor Tracker data"""
    data = download_dt_data()
    with open(config.PATHS.data + "/dt_articles.json", "w") as f:
        json.dump(data, f)


def read_dt_data() -> json:
    """Read Donor Tracker data"""
    with open(config.PATHS.data + "/dt_articles.json", "r") as f:
        data = json.load(f)
    return data


def _clean_content(content: str) -> str:
    """Remove the markdown formatting in favour of plain text"""

    import re

    # remove acronym notes, highlight ukraine and remove titles, breaklines, etc
    new_content = (
        re.sub(r":abbr\[(.*?)\]", r"\1", content)
        .replace("Ukraine", "<strong>Ukraine</strong>")
        .replace(r"**", "")
        .replace(r"##", "")
        .replace("  ", " ")
        .replace(r"\\n", "")
    )

    # Clean the output
    new_content = re.sub(rf".\\", "", new_content)
    new_content = re.sub(r"\n", " .", new_content)
    new_content = re.sub(r"\\", "", new_content)
    new_content = re.sub(r'\[(.*?)\]\( "(.*?)"\)', r"\1 (\2)", new_content)

    return new_content.replace(" . .", ". ")


def _shorten_content(content: str, char_count: int = 200) -> str:
    """Shorten content to char_count characters"""

    if len(content) > char_count:
        return _clean_content(content[:char_count] + "...")
    return _clean_content(content)


def _convert_slug(df: pd.DataFrame) -> str:
    """Convert slug to Donor Tracker URL and encapsulate in a "read more" link"""
    return (
        f'<strong><a href="https://donortracker.org/policy_updates?policy={df.slug}" '
        f'target="_blank" rel="noopener noreferrer">'
        f"read more</a></strong>"
    )


def title_break_date(df: pd.DataFrame) -> str:
    """Make a column with title linebreak date"""
    return f"<strong>{df.title}</strong><br>{df.publish_date}"


def clean_dt_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean Donor Tracker data"""

    return (
        df.assign(
            publish_date=pd.to_datetime(df.publish_date).dt.strftime("%d %b %Y"),
            content=lambda d: d.content.apply(_shorten_content, char_count=200),
            read_more=lambda d: d.apply(_convert_slug, axis=1),
            title_date=lambda d: d.apply(title_break_date, axis=1),
        )
        .filter(["title_date", "content", "read_more"], axis=1)
        .assign(content=lambda d: d.content + " " + d.read_more)
        .drop("read_more", axis=1)
        .rename(columns={"title_date": "", "content": ""})
    )


def dt_data_to_df(dt_data: json) -> pd.DataFrame:
    """Convert Donor Tracker data to a DataFrame"""
    df = pd.DataFrame(dt_data["data"]).pipe(clean_dt_data)

    return df
