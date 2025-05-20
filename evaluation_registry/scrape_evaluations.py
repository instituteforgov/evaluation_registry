# %%
# -*- coding: utf-8 -*-

"""
    Purpose
        Scrape evaluations from the Evaluation Registry
    Inputs
        - html: https://evaluation-registry.cabinetoffice.gov.uk/search/?search_term=
    Outputs
        - pkl: url_20250402.pkl
            - URLs for individual evaluations
        - pkl: evaluationdetails_20250402.pkl
            - Details of evaluations
    Notes
        - Using the Evaluation Registry search function without a search term results
        in duplicate evaluations being returned - these are weededed out below
        - Where an event features multiple times - e.g. "Publication of final results" -
        we keep the latest date
# we use the search function
"""

# flake8: noqa

import datetime
import re

from bs4 import BeautifulSoup as bs
from IPython.display import display     # noqa: E402
import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry

# %%
# SET UP REQUESTS
session = requests.Session()
retry = Retry(connect=5, backoff_factor=0.5)
adapter = HTTPAdapter(max_retries=retry)
session.mount("https://", adapter)

# %%
# GRAB PAGE URLS
# NB: Non-zero based page indexing
URL = "https://evaluation-registry.cabinetoffice.gov.uk/search/?page="
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"     # noqa: E501
}

urlList = []

next_page = True
page = 1

print(datetime.datetime.now())
while next_page:

    req = session.get(URL + str(page), headers=headers)
    soup = bs(req.text, "html.parser")

    cards = soup.find("div", {"aria-label": "Evaluation Registry search results"})
    card_urls = cards.find_all("a", {
        "class": "govuk-link",
    })

    # Restrict to links starking with "/search/"
    card_urls = [url for url in card_urls if re.match(r"^/search/", url["href"])]

    for url in card_urls:
        if "href" in url.attrs:
            urlList.append(url.attrs["href"])

    if soup.find("div", {"class": "govuk-pagination__next"}):
        next_page = True
    else:
        next_page = False

    page += 1
print(datetime.datetime.now())

# Save URLs to pickle
urlList = list(set(urlList))
pd.DataFrame(urlList).to_pickle("urls_20250402.pkl")


# %%
def get_details(
    partial: str
) -> dict[str, str]:
    """Grab evaluation details"""

    url = "https://evaluation-registry.cabinetoffice.gov.uk" + str(partial)

    details = {}

    content = session.get(url, headers=headers)
    soup = bs(content.text, "html.parser")

    details["url"] = url
    details["title"] = soup.find("h1", {"class": "govuk-heading-l"}).text.strip()
    if details["title"] == "Page not found":
        print("Page not found: " + url)
        return details

    details["description"] = soup.find(
        "p", {"class": "govuk-body govuk-grid-column-two-thirds govuk-!-padding-0"}
    ).text.strip()

    rows = soup.find_all("div", {"class": "govuk-summary-list__row"})
    for row in rows:
        key = row.find("dt", {"class": "govuk-summary-list__key"}).text.strip()
        value = row.find("dd", {"class": "govuk-summary-list__value"}).text.strip()

        # Needed to handle keys that can appear multiple times as with Event Dates
        if key not in details:
            details[key] = value
        else:
            details[key] += ", " + value

    return details


# %%
# EXTRACT DETAILS
details_list = []

print(datetime.datetime.now())
for partial in urlList:
    details = get_details(partial)
    details_list.append(details)
print(datetime.datetime.now())

df_details = pd.DataFrame(details_list)

display(df_details)

# %%
# SAVE TO PICKLE
df_details.to_pickle("evaluationdetails_20250414.pkl")

# %%
# Drop "Page not found" rows
df_details = df_details.loc[
    df_details["title"] != "Page not found"
].reset_index(drop=True)

# %%
# Drop duplicates
# NB: These seem genuine duplicates that have come in from the way in which
# we use the search function
df_details = df_details.loc[
    ~df_details.duplicated()
].reset_index(drop=True)

# %%
# EDIT DATA
# Remove "Closed organisation: " from specified columns
df_details["Lead department"] = df_details["Lead department"].str.replace(
    "Closed organisation: ", "", regex=False
)
df_details["Other departments"] = df_details["Other departments"].str.replace(
    "Closed organisation: ", "", regex=False
)

# %%
# Clean "Evalueation types" column
expected_evaluation_types = [
    "Impact evaluation",
    "Process evaluation",
    "Value for money evaluation",
    "Other",
]

df_details["Evaluation types"] = df_details["Evaluation types"].str.replace(
    "\n                \n                  ", ", ", regex=False
)

# %%
# Check "Evaluation types" values are as expected
df_details.loc[
    df_details["Evaluation types"] == "",
    "Evaluation types"
] = df_details.loc[
    df_details["Evaluation types"] == "",
    "Evaluation types"
].replace(r'^\s*$', pd.NA, regex=True)

assert set(
    df_details.loc[
        df_details["Evaluation types"].notna()
    ]["Evaluation types"].str.split(", ", expand=False).explode().unique()
) == set(expected_evaluation_types)

# %%
# Convert "Evaluation types" to separate columns for each evaluation type
pos = df_details.columns.get_loc("Evaluation types")

i = 1

for evaluation_type in expected_evaluation_types:
    df_details.insert(pos + i, evaluation_type, None)
    df_details[evaluation_type] = df_details["Evaluation types"].str.contains(evaluation_type)
    i += 1

# %%
df_details.loc[
    df_details["Event Name"].notna()
]["Event Name"].str.split(", ", expand=False).explode().unique()

# %%
# Check "Event name" values are as expected
expected_event_names = [
    "Intervention start date",
    "Intervention end date",
    "Evaluation start",
    "Evaluation end",
    "Final data analysis end",
    "Publication of interim results",
    "Publication of final results",
    "Not Set",
    "Other",
]

assert set(
    df_details.loc[
        df_details["Event Name"].notna()
    ]["Event Name"].str.split(", ", expand=False).explode().unique()
) == set(expected_event_names)

# %%
# Split "Event Name" and "Event date" by ", "
df_details["Event Name split"] = df_details["Event Name"].str.split(", ")
df_details["Event date split"] = df_details["Event date"].str.split(", ")

# %%
# Explode and create as new df
df_dates = df_details[["Event Name split", "Event date split"]].explode(
    ["Event Name split", "Event date split"]
)

# %%
# Convert to datetime to allow sorting
df_dates["Event date split"] = df_dates["Event date split"].apply(
    lambda x: pd.to_datetime(x, errors="coerce", format="%B %Y")
)

# %%
# Drop duplicates, preserving oldest date
# NB: reset_index() in order to include the index in drop_duplicates()
df_dates = df_dates.reset_index().sort_values(
    ["index", "Event date split"],
).drop_duplicates(
    ["index", "Event Name split"],
    keep="last",
)
df_dates.set_index("index", inplace=True)

# %%
# Format date
df_dates["Event date split"] = df_dates["Event date split"].dt.strftime("%Y-%m")

# %%
# Pivot
df_dates_pivot = df_dates.loc[
    df_dates["Event Name split"].notna()
].pivot(
    columns="Event Name split",
    values="Event date split"
)

# %%
# Reorder cols to match expected_event_names
df_dates_pivot = df_dates_pivot.reindex(
    expected_event_names,
    axis=1,
)

# %%
# Prepend 'Event - ' to column names
df_dates_pivot.columns = [
    "Event - " + c for c in df_dates_pivot.columns.tolist()
]

# %%
# Join df_dates_pivot back to df_details
df_details = df_details.join(
    df_dates_pivot
)

# %%
# Flag rows where there is a repeated "Event Name split" in df_details
df_details.insert(
    df_details.columns.get_loc("Event date") + 1,
    "Event Name duplicated",
    False,
)
df_details.loc[
    df_details["Event Name split"].apply(
        lambda x: len(x) if isinstance(x, list) else 0
    ) != df_details["Event Name split"].apply(
        lambda x: len(set(x)) if isinstance(x, list) else 0
    ),
    "Event Name duplicated"
] = True

# %%
# Move date columns to the right of "Event Name duplicated" column
expected_event_names = [
    "Event - " + c for c in expected_event_names
]
df_details = df_details[
    df_details.columns[:df_details.columns.get_loc("Event Name duplicated") + 1].tolist()
    + expected_event_names + [
        c for c in df_details.columns[
            df_details.columns.get_loc("Event Name duplicated") + 1:
        ].tolist() if c not in expected_event_names
    ]
]

# %%
# Drop "Event Name split" and "Event date split" columns
df_details.drop(
    ["Event Name split", "Event date split"],
    axis=1,
    inplace=True,
)

# %%
df_details
