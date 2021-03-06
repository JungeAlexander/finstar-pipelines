import logging
from pathlib import Path
from time import sleep
from typing import List, Tuple

import finviz
import numpy as np
import pandas as pd
import pandas_market_calendars as mcal
import pendulum
import requests
import yfinance as yf
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils import timezone
from newsapi import NewsApiClient
from newsapi.newsapi_exception import NewsAPIException
from pendulum.date import Date

logger = logging.getLogger(__name__)

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "airflow",
    "retries": 0,
}


def get_start_end_dates(
    exec_date: Date, minimal_period_days: int = 0
) -> Tuple[Date, str, Date, str]:
    subtract_days = max(minimal_period_days, int(Variable.get("sp500_recent_days")))
    period = exec_date - exec_date.subtract(days=subtract_days)
    start_date = period.start.date()
    start_date_str = start_date.to_date_string()
    end_date = period.end.date()
    end_date_str = end_date.to_date_string()
    return start_date, start_date_str, end_date, end_date_str


@dag(
    default_args=default_args,
    schedule_interval="50 7 * * *",
    start_date=pendulum.parse("20210715"),
    catchup=True,
    max_active_runs=1,
)
def sp500_dag():
    """
    ### Download S&P 500 data
    First, determine symbols comprising S&P 500 from Wikipedia.
    Second, download recent market data for all symbols from YFinance.
    Third, enrich with financial news headlines from finviz.
    """

    @task()
    def get_ticker_symbols() -> List[str]:
        """
        #### Get S&P 500 ticker symbols as available from Wikipedia
        Returns a list of ticker symbols, as strings.
        """
        wiki_df = pd.read_html(
            "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        )[0]
        # replace . with - in symbols as listed in yfinance
        symbols = sorted((s.replace(".", "-") for s in wiki_df.loc[:, "Symbol"]))
        # TODO: check below via great expectations etc?
        if len(symbols) != len(set(symbols)):
            raise ValueError("S&P500 contains duplicated symbols")
        return symbols

    @task()
    def get_ticker_data(sp500_symbols: List[str], **context):
        """
        #### Get ticker data from Yfinance
        Only market days are fetched and data are stored locally as Parquet files,
        partitioned by stock symbol.
        """
        start_date, start_date_str, end_date, end_date_str = get_start_end_dates(
            context["execution_date"]
        )
        # determine market days in period
        market_cal = mcal.get_calendar("NYSE")
        market_days = market_cal.schedule(
            start_date=start_date, end_date=end_date
        ).index
        market_days = {Date(year=n.year, month=n.month, day=n.day) for n in market_days}

        ticker_interval = f"{int(Variable.get('sp500_ticker_interval_minutes'))}m"

        # for each symbol, fetch data and store them as parquet files locally
        output_dir = Path(Variable.get("sp500_output_dir")) / "ticker_data"
        for s in sp500_symbols:
            logger.info(f"Processing {s}")
            ticker = yf.Ticker(s)
            ticker_df = ticker.history(
                start=start_date_str,
                end=end_date.add(days=1).to_date_string(),  # yf interval is right-open
                interval=ticker_interval,
            )
            # TODO: check above for at least some data for market days via great expectations etc?
            # To determine if a day is a market day, use:
            # for current_date in period:
            #     if current_date.date() in market_days:

            ticker_df = ticker_df.tz_convert("UTC", level=0)
            ticker_df.reset_index(inplace=True)
            ticker_df.insert(0, "Symbol", s)

            current_out_dir = output_dir / f"Symbol={s}"
            current_out_dir.mkdir(parents=True, exist_ok=True)
            ticker_df.to_parquet(
                current_out_dir / f"{start_date_str}_{end_date_str}.snappy.parquet",
                engine="pyarrow",
                compression="snappy",
            )
            sleep(1)

    @task()
    def get_news_finviz(sp500_symbols: List[str], **context):
        """
        #### Get news from finviz
        """
        start_date, start_date_str, end_date, end_date_str = get_start_end_dates(
            context["execution_date"]
        )
        # Generate UTC datetime for given start and end days for comparison later
        start_datetime = pendulum.datetime(
            year=start_date.year,
            month=start_date.month,
            day=start_date.day,
        )
        end_datetime = pendulum.datetime(
            year=end_date.year,
            month=end_date.month,
            day=end_date.day,
            hour=23,
            minute=59,
            second=59,
            microsecond=999999,
        )

        # for each symbol, fetch news for each  day and store them as parquet
        # files locally
        output_dir = Path(Variable.get("sp500_output_dir")) / "news" / "finviz"
        for s in sp500_symbols:
            logger.info(f"Processing {s}")

            try:
                news_fv = finviz.get_news(s)
            except requests.exceptions.HTTPError as e:
                logger.warning(f"Failed for {s}")
                continue
            news_fv_df = pd.DataFrame(news_fv)
            # TODO: check above for at least some news returned via great expectations etc?
            news_fv_df.columns = ["Datetime", "Title", "URL", "Source"]
            news_fv_df.insert(0, "Symbol", s)
            news_fv_df.insert(3, "Description", pd.NA)
            news_fv_df.insert(5, "Author", pd.NA)
            news_fv_df = news_fv_df[
                [
                    "Symbol",
                    "Datetime",
                    "Title",
                    "Description",
                    "Source",
                    "Author",
                    "URL",
                ]
            ]
            news_fv_df["Datetime"] = pd.to_datetime(news_fv_df["Datetime"])
            news_fv_df["Datetime"] = news_fv_df["Datetime"].dt.tz_localize("US/Eastern")
            news_fv_df["Datetime"] = news_fv_df["Datetime"].dt.tz_convert("UTC")
            news_fv_df = news_fv_df.loc[
                (
                    np.logical_and(
                        start_datetime <= news_fv_df["Datetime"],
                        news_fv_df["Datetime"] <= end_datetime,
                    )
                ),
                :,
            ]
            news_fv_df.sort_values(by=["Datetime"], inplace=True, ascending=True)
            news_fv_df.reset_index(inplace=True, drop=True)

            current_out_dir = output_dir / f"Symbol={s}"
            current_out_dir.mkdir(parents=True, exist_ok=True)
            news_fv_df.to_parquet(
                current_out_dir / f"{start_date_str}_{end_date_str}.snappy.parquet",
                engine="pyarrow",
                compression="snappy",
            )
            sleep(1)

    @task()
    def get_news_newsapi(sp500_symbols: List[str], **context):
        """
        #### Get news from News API
        """
        start_date, start_date_str, end_date, end_date_str = get_start_end_dates(
            context["execution_date"], minimal_period_days=7
        )
        # for each symbol, fetch news for each  day and store them as parquet
        # files locally
        output_dir = Path(Variable.get("sp500_output_dir")) / "news" / "newsapi"
        client = NewsApiClient(api_key=Variable.get("news_api_key"))
        # determine index of symbol fetched last in prev. run or start from scratch
        start_index = 0
        last_symbol = Variable.get("sp500_newsapi_last_symbol", default_var=None)
        if last_symbol is not None:
            for i, s in enumerate(sp500_symbols):
                if s == last_symbol:
                    start_index = (i + 1) % len(sp500_symbols)
                    break
        for i in range(len(sp500_symbols)):
            ix = (start_index + i) % len(sp500_symbols)
            s = sp500_symbols[ix]
            logger.info(f"Processing {s}")
            try:
                now = timezone.utcnow()
                news_api = client.get_everything(
                    q=s, from_param=start_date_str, to=end_date_str, language="en"
                )
            except NewsAPIException as e:
                if e.get_code() == "rateLimited":
                    return
                else:
                    raise e
            news_api_list = []
            for a in news_api["articles"]:
                source = a["source"]["name"]
                author = a["author"]
                title = a["title"]
                description = a["description"]
                url = a["url"]
                published = a["publishedAt"]
                requested = now
                news_api_list.append(
                    (s, published, title, description, source, author, url, requested)
                )
            news_api_df = pd.DataFrame(
                news_api_list,
                columns=[
                    "Symbol",
                    "Datetime",
                    "Title",
                    "Description",
                    "Source",
                    "Author",
                    "URL",
                    "Requested_Datetime",
                ],
            )
            news_api_df["Datetime"] = pd.to_datetime(news_api_df["Datetime"])
            news_api_df.sort_values(by=["Datetime"], inplace=True, ascending=True)
            news_api_df.reset_index(inplace=True, drop=True)

            current_out_dir = output_dir / f"Symbol={s}"
            current_out_dir.mkdir(parents=True, exist_ok=True)
            news_api_df.to_parquet(
                current_out_dir / f"{start_date_str}_{end_date_str}.snappy.parquet",
                engine="pyarrow",
                compression="snappy",
            )
            # store that this symbol has been processed for coming runs
            Variable.delete("sp500_newsapi_last_symbol")
            Variable.set("sp500_newsapi_last_symbol", s)
            logger.info(f"Finished {s}")
            sleep(1)

    ticker_symbols = get_ticker_symbols()
    get_ticker_data(ticker_symbols)
    get_news_finviz(ticker_symbols)
    get_news_newsapi(ticker_symbols)


d = sp500_dag()
