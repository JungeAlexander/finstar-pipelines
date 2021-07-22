import logging
from pathlib import Path
from time import sleep
from typing import Dict, List, Tuple

import finviz
import numpy as np
import pandas as pd
import pandas_market_calendars as mcal
import pendulum
import yfinance as yf
from airflow.decorators import dag, task
from airflow.models import Variable
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


def get_start_end_dates(exec_date: Date) -> Tuple[Date, str, Date, str]:
    period = exec_date - exec_date.subtract(days=int(Variable.get("sp500_recent_days")))
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
def sp500_newsapi_dag():
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
    def get_news_newsapi(sp500_symbols: List[str], **context):
        """
        #### Get news from News API
        """
        start_date, start_date_str, end_date, end_date_str = get_start_end_dates(
            context["execution_date"]
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
                    start_index = i
                    break
        for s in sp500_symbols[i:]:  # FIXME: restart from scratch?
            logger.info(f"Processing {s}")
            Variable.set("sp500_newsapi_last_symbol", s)

    ticker_symbols = get_ticker_symbols()
    get_news_newsapi(ticker_symbols)


d = sp500_newsapi_dag()
