# %%
# !/usr/bin/env python
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
# we use the search function
"""

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
) -> None:
    """Grab evaluation details"""

    url = "https://evaluation-registry.cabinetoffice.gov.uk" + str(partial)

    details = {}

    content = session.get(url, headers=headers)
    soup = bs(content.text, "html.parser")

    details["url"] = url
    details["title"] = soup.find("h1", {"class": "govuk-heading-l"}).text.strip()
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
df_details.to_pickle("evaluationdetails_20250402.pkl")

# %%
# Drop duplicates
# NB: These seem genuine duplicates that have come in from the way in which
# we use the search function
df_details = df_details.loc[
    ~df_details.duplicated()
]

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
# Convert "Evaluation types" to separate Impact evaluation, Process evaluation, Value for
# money evaluation, Other columns
df_details["Evaluation types"] = df_details["Evaluation types"].str.replace(
    "\n                \n                  ", ", ", regex=False
)

pos = df_details.columns.get_loc("Evaluation types")

i = 1

for evaluation_type in [
    "Impact evaluation",
    "Process evaluation",
    "Value for money evaluation",
    "Other",
]:
    df_details.insert(pos + i, evaluation_type, None)
    df_details[evaluation_type] = df_details["Evaluation types"].str.contains(evaluation_type)
    i += 1

# %%
