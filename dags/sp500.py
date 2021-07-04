# 1-sp500 symbols from wikipedia -> pass list of str to below
# How to do fork in airflow?
#
# 2- ticker data
# partition by : symbol  one file per day, frequency 1m
# columns: symbol, datetime (UTC), value
#
# 3-news for samples from finviz (double-check gamstonk terminal for better sources)
# (https://towardsdatascience.com/stock-news-sentiment-analysis-with-python-193d4b4378d4)
# columns: symbol, datetime (UTC), source, link, sentiment_score, headline
# add sentiment model from nltk or spacy
