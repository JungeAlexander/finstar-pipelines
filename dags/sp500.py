from typing import List

import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils.dates import parse_execution_date

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
        exec_date = context["execution_date"]
        time_frame = pendulum.duration(days=int(Variable.get("sp500_recent_days")))

        recent_episodes_date = exec_date - time_frame
        return [str(recent_episodes_date)]

    @task()
    def get_ticker_data(sp500_symbols: List[str]):
        """
        #### Get ticker data
        TODO
        partition by : symbol  one file per day, frequency 1m
        columns: symbol, datetime (UTC), value
        """
        pass

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
