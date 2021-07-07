import logging
from time import sleep
from typing import List

import pandas as pd
import pendulum
import yfinance as yf
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils.dates import parse_execution_date

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
    TODO
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
        symbols = sorted(wiki_df.loc[:, "Symbol"])
        # TODO: check below GE?
        if len(symbols) != len(set(symbols)):
            raise ValueError("S&P500 contains duplicated symbols")
        return symbols

    @task()
    def get_ticker_data(sp500_symbols: List[str], **context):
        """
        #### Get ticker data
        TODO
        partition by : symbol  one file per day,
        columns: symbol, datetime (UTC), value
        """
        exec_date = context["execution_date"]
        time_frame = pendulum.duration(days=int(Variable.get("sp500_recent_days")))
        ticker_interval = f"{int(Variable.get('sp500_ticker_interval_minutes'))}m"

        recent_episodes_date = exec_date - time_frame

        for s in sp500_symbols:
            logger.info(f"Processing {s}")
            ticker = yf.Ticker(s)
            ticker_df = ticker.history(
                start=recent_episodes_date.to_date_string(),
                end=exec_date.to_date_string(),
                interval=ticker_interval,
            )
            ticker_df.to_parquet(
                f"{s}.snappy.parquet", engine="pyarrow", compression="snappy"
            )
            sleep(1)
            return ticker_df  # TODO: how to save files?

    @task()
    def get_news(sp500_symbols: List[str]):
        """
        #### Get news
        TODO
        news for samples from finviz (double-check gamstonk terminal for better sources)
        (https://towardsdatascience.com/stock-news-sentiment-analysis-with-python-193d4b4378d4)
        columns: symbol, datetime (UTC), source, link, sentiment_score, headline
        add sentiment model from nltk or spacy
        """
        pass

    ticker_symbols = get_ticker_symbols()
    get_ticker_data(ticker_symbols)
    get_news(ticker_symbols)


d = sp500_dag()
