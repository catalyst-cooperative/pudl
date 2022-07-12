from collections.abc import Generator
import re
from pathlib import Path
from zipfile import ZipFile

import pandas as pd
import requests

# caused attribute errors on import???
# from pudl.helpers import download_zip_url


def download_zip_url(url, save_path, chunk_size=128):  # copied from pudl.helpers
    """Download and save a Zipfile locally.

    Useful for acquiring and storing non-PUDL data locally.

    Args:
        url (str): The URL from which to download the Zipfile
        save_path (pathlib.Path): The location to save the file.
        chunk_size (int): Data chunk in bytes to use while downloading.

    Returns:
        None

    """
    # This is a temporary hack to avoid being filtered as a bot:
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:77.0) Gecko/20100101 Firefox/77.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }
    r = requests.get(url, stream=True, headers=headers)
    with save_path.open(mode="wb") as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            fd.write(chunk)


def generate_links() -> Generator[str, None, None]:
    URL_BASE = "https://www.eia.gov/electricity/monthly/archive/"
    # # 2010 and earlier
    # https://www.eia.gov/electricity/monthly/archive/xls/epm0409.zip
    for year in range(2008, 2011):
        for month in range(1, 13):
            yield f"{URL_BASE}/xls/epm{str(month).zfill(2)}{str(year)[-2:]}.zip"
    # 2011 and later
    # https://www.eia.gov/electricity/monthly/archive/march2011.zip
    for year in range(2011, 2022):
        for month in [
            "january",
            "february",
            "march",
            "april",
            "may",
            "june",
            "july",
            "august",
            "september",
            "october",
            "november",
            "december",
        ]:
            yield f"{URL_BASE}/{month}{year}.zip"
    for month in [  # 14 month delay due to revised data. This completes 2020
        "january",
        "february",
    ]:
        yield f"{URL_BASE}/{month}{2022}.zip"


class Extractor:
    FILENAMES = {  # monthly by fuel. Two values due to naming convention change
        "coal": ("Table_4_10_A.xlsx", "epmxlfile4_10_a.xls"),
        "oil": ("Table_4_11_A.xlsx", "epmxlfile4_11_a.xls"),
        "petcoke": ("Table_4_12_A.xlsx", "epmxlfile4_12_a.xls"),
        "gas": ("Table_4_13_A.xlsx", "epmxlfile4_13_a.xls"),
        "coal_ytd": ("Table_4_10_B.xlsx", "epmxlfile4_10_b.xls"),
        "oil_ytd": ("Table_4_11_B.xlsx", "epmxlfile4_11_b.xls"),
        "petcoke_ytd": ("Table_4_12_B.xlsx", "epmxlfile4_12_b.xls"),
        "gas_ytd": ("Table_4_13_B.xlsx", "epmxlfile4_13_b.xls"),
    }

    def __init__(self, dir_with_zips: Path) -> None:
        assert dir_with_zips.exists()
        self.root = dir_with_zips

    def extract(self) -> dict[str, pd.DataFrame]:
        zip_files = self.root.glob("*.zip")
        monthly_dfs = {key: [] for key in Extractor.FILENAMES.keys()}
        for zip_file in zip_files:
            with ZipFile(zip_file, "r") as archive:
                # look for occasional nested subdirectory
                prefix = (
                    archive.filelist[0].filename
                    if archive.filelist[0].filename.endswith("/")
                    else None
                )
                for fuel, file_names in Extractor.FILENAMES.items():
                    for i, file_name in enumerate(file_names):
                        if prefix is not None:
                            file_name = prefix + file_name
                        try:
                            file = archive.open(file_name, "r")
                        except KeyError as err:
                            # if both fail raise the error
                            if i == 0:
                                continue
                            else:
                                raise err
                        break
                    is_ytd = (
                        file_name.rsplit(".")[0][-1].lower() == "b"
                    )  # last character is 'b'
                    df = _read_state_month_excel(file, is_ytd=is_ytd)
                    file.close()
                    monthly_dfs[fuel].append(df)
        monthly_dfs = {
            key: pd.concat(dfs, axis=0).sort_index() for key, dfs in monthly_dfs.items()
        }
        return monthly_dfs


def _parse_dates_from_old_ytd_files(raw_df: pd.DataFrame) -> dict[int, pd.Timestamp]:
    title_row = raw_df[raw_df[0].str.match("^Table 4.1[0123].B").fillna(False)].index[0]
    title_text = raw_df.at[title_row, 0]
    regex = r".+ (?P<month>\w+) (?P<last_year>\d{4}) and (?P<first_year>\d{4})$"
    match = re.match(regex, title_text)
    last_date = pd.Timestamp(f"{match.group('month')} {match.group('last_year')}")
    first_date = pd.Timestamp(f"{match.group('month')} {match.group('first_year')}")
    return {last_date.year: last_date, first_date.year: first_date}


def _read_state_month_excel(file, is_ytd: bool) -> pd.DataFrame:
    df = pd.read_excel(file, header=None)
    if is_ytd:
        date_replace_dict = _parse_dates_from_old_ytd_files(df)
    else:
        date_replace_dict = {}
    # get rid of header and footer (lengths change slightly over time)
    first_value_idx = df[df[0].str.startswith("Census Division").fillna(False)].index[0]
    last_value_idx = df[df[0].str.endswith("Total").fillna(False)].index[0]
    df = df.loc[first_value_idx:last_value_idx, :]

    # separate out percent change column
    assert df.iat[1, 3] in {"Percent Change", "Percentage Change"}
    pct_change = df.iloc[2:, [0, 3]].rename(columns={0: "region", 3: "yoy_pct_change"})
    df = df.drop(3, axis=1)  # drop percent change column

    # set multiindex columns
    df = df.rename(columns={0: "region"}).set_index("region")
    if isinstance(df.iat[1, 1], str):  # check if dates were automatically parsed yet
        dates = df.iloc[1, :].str.replace(" YTD", "")  # make YTD dates parsable
    else:
        dates = df.iloc[1, :].replace(date_replace_dict)
    cols = pd.MultiIndex.from_frame(
        pd.DataFrame(
            {
                "sector": df.iloc[0, :].fillna(method="ffill"),
                "date": pd.to_datetime(dates),
            }
        )
    )
    df.columns = cols
    df = df.iloc[2:, :]  # drop rows that became column index
    df = df.stack(level=1)

    # mark the older date column as revised, final values (the newer one is provisional)
    min_date = df.index.get_level_values("date").min()
    df["is_revised"] = df.index.get_level_values("date") == min_date

    pct_change["date"] = df.index.get_level_values("date").max()
    pct_change = pct_change.set_index(["region", "date"])
    out = pd.concat([df, pct_change], axis=1)
    return out


def transform(extracted_dfs: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    pass


def add_fuel_cols_and_combine(
    extracted_dfs: dict[str, pd.DataFrame]
) -> dict[str, pd.DataFrame]:
    # add fuel column
    monthly = []
    ytd = []
    for key, df in extracted_dfs.items():
        if key.endswith("ytd"):
            ytd.append(df.assign(fuel=key.split("_")[0]))
        else:
            monthly.append(df.assign(fuel=key))
    monthly = (
        pd.concat(monthly, axis=0)
        .set_index(["fuel", "is_revised"], append=True)
        .unstack("is_revised")
        .dropna(axis=1, how="all")
    )
    ytd = (
        pd.concat(ytd, axis=0)
        .set_index(["fuel", "is_revised"], append=True)
        .unstack("is_revised")
        .dropna(axis=1, how="all")
    )
    return {"monthly": monthly, "ytd": ytd}


def download_all() -> None:
    for link in generate_links():
        file_name = link.rsplit("/", maxsplit=1)[-1]
        save_path = Path("./eia_epm_zips") / file_name
        download_zip_url(link, save_path=save_path, chunk_size=1024)


if __name__ == "__main__":
    zip_dir = Path(__file__).parent / "eia_epm_zips/"
    assert zip_dir.exists()
    dfs = Extractor(zip_dir).extract()
    print("yay")
# URL_BASE = "https://api.eia.gov/v2/"
"""
Notes:
API doesn't have IPP or Census Region breakouts, and Oil prices are hard to access (multiple pieces).
So I decided to use EIA EPM instead, even though it comes in a cumbersome format.
- won't return more than 5k rows (check value "total" in response)
- use route /data to get column info.
    - Add &data[]=<col_name> to get actual data (append multiple to get more columns)
    - Leave /data out to get table metadata.
- use route /facet/<name> to get subset metadata. See available facets in metadata
    - for getting data, append &facets[stateid][]=CO, for example
    - use &frequency=monthly (or other freq. Check the metadata)
- filter time with start and end params, eg &start=2008-01-31.
    - Check metadata for date format. Maybe have to leave off days for monthly data, for both for annual.
- use &offset=<n rows>&length=<k rows> to paginate large results

freq = ['monthly', 'annual']  # hierarchy
spatial_res = ['state', 'census', 'national']  # hierarchy
fuel = ['coal', 'gas', 'oil']  # categories
sector = ['utility', 'ipp', 'industial']  # categories
# two decimal precision

In January 2013, the threshold was changed to 200 megawatts for plants primarily fueled by natural
gas, petroleum coke, distillate fuel oil, and residual fuel oil. The requirement to report self‐ produced
and minor fuels, i.e., blast furnace gas, other manufactured gases, kerosene, jet fuel, propane, and
waste oils was eliminated. The threshold for coal plants remained at 50 megawatts.

U.S. Energy Information Administration | Electric Power Monthly Page 14
Not all data are collected monthly on the Form EIA‐923. Beginning with 2008 data, a sample of the
respondents report monthly, with the remainder reporting annually. Until January 2013, monthly fuel
receipts values for the annual surveys were imputed via regression.
"""
