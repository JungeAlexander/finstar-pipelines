import logging
from pathlib import Path
from time import sleep
from typing import List

import finviz
import numpy as np
import pandas as pd
import pandas_market_calendars as mcal
import pendulum
import yfinance as yf
from airflow.decorators import dag, task
from airflow.models import Variable

logger = logging.getLogger(__name__)

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "airflow",
    "retries": 0,
}


@dag(
    default_args=default_args,
    schedule_interval=Variable.get("sp500_update_schedule"),
    start_date=pendulum.parse(Variable.get("sp500_start_date")),
    catchup=True,
)
def sp500_dag():
    """
    ### Download S&P 500 data
    First, determine symbols comprising S&P 500 from Wikipedia.
    Second, download recent market data for all symbols from YFinance.
    Third, enrich with financial news headlines from finviz.
    """

    @task()
    def get_ticker_symbols(**context) -> List[str]:
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
        # determine period of interest and market days in period
        exec_date = context["execution_date"]
        period = exec_date - exec_date.subtract(
            days=int(Variable.get("sp500_recent_days"))
        )
        market_cal = mcal.get_calendar("NYSE")
        market_days = market_cal.schedule(
            start_date=period.start.date(), end_date=period.end.date()
        ).index
        market_days = {
            pendulum.Date(year=n.year, month=n.month, day=n.day) for n in market_days
        }

        ticker_interval = f"{int(Variable.get('sp500_ticker_interval_minutes'))}m"

        # for each symbol, fetch data for each market day and store them as parquet
        # files locally
        output_dir = Path(Variable.get("sp500_output_dir")) / "ticker_data"
        for s in sp500_symbols:
            logger.info(f"Processing {s}")
            ticker = yf.Ticker(s)
            for current_date in period:
                if current_date.date() not in market_days:
                    continue
                current_date_string = current_date.to_date_string()
                next_date_string = (
                    current_date + pendulum.duration(days=1)
                ).to_date_string()
                ticker_df = ticker.history(
                    start=current_date_string,
                    end=next_date_string,
                    interval=ticker_interval,
                )
                # TODO: check above for at least some data via great expectations etc?

                ticker_df = ticker_df.tz_convert("UTC", level=0)
                ticker_df.reset_index(inplace=True)
                ticker_df.insert(0, "Symbol", s)

                current_out_dir = output_dir / f"Symbol={s}"
                current_out_dir.mkdir(parents=True, exist_ok=True)
                ticker_df.to_parquet(
                    current_out_dir / f"{current_date_string}.snappy.parquet",
                    engine="pyarrow",
                    compression="snappy",
                )
                sleep(1)

    @task()
    def get_news_finviz(sp500_symbols: List[str], **context):
        """
        #### Get news from finviz
        """
        exec_date = context["execution_date"]
        period = exec_date - exec_date.subtract(
            days=int(Variable.get("sp500_recent_days"))
        )
        # for each symbol, fetch news for each  day and store them as parquet
        # files locally
        output_dir = Path(Variable.get("sp500_output_dir")) / "news" / "finviz"
        for s in sp500_symbols:
            logger.info(f"Processing {s}")
            for current_datetime in period:
                # Generate UTC datetime for given day with hours, min, sec as zero
                current_date = pendulum.datetime(
                    year=current_datetime.year,
                    month=current_datetime.month,
                    day=current_datetime.day,
                )
                news_fv = finviz.get_news(s)
                news_fv_df = pd.DataFrame(news_fv)
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
                news_fv_df["Datetime"] = news_fv_df["Datetime"].dt.tz_localize(
                    "US/Eastern"
                )
                news_fv_df["Datetime"] = news_fv_df["Datetime"].dt.tz_convert("UTC")
                news_fv_df = news_fv_df.loc[
                    (
                        np.logical_and(
                            current_date <= news_fv_df["Datetime"],
                            news_fv_df["Datetime"]
                            < current_date + pendulum.duration(days=1),
                        )
                    ),
                    :,
                ]
                news_fv_df.sort_values(by=["Datetime"], inplace=True, ascending=True)
                news_fv_df.reset_index(inplace=True, drop=True)

                current_out_dir = output_dir / f"Symbol={s}"
                current_out_dir.mkdir(parents=True, exist_ok=True)
                news_fv_df.to_parquet(
                    current_out_dir
                    / f"{current_date.date().to_date_string()}.snappy.parquet",
                    engine="pyarrow",
                    compression="snappy",
                )
                sleep(1)

    @task()
    def get_news_newsapi(sp500_symbols: List[str], **context):
        """
        #### Get news from News API
        """
        pass

    ticker_symbols = get_ticker_symbols()
    get_ticker_data(ticker_symbols)
    get_news_finviz(ticker_symbols)
    get_news_newsapi(ticker_symbols)


d = sp500_dag()
