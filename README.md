# finstar pipelines

🚧 WIP 🚧

## Installation

Python environment:

```
poetry install --no-root
poetry run pre-commit install
```

Airflow (via Astronomer):

```
brew install astronomer/tap/astro@0.25.1
astro version
# Astro CLI Version: 0.25.1, Git Commit: ab3af105f967105fa23e6c317c474612803b21e2
```

AWS CDK as described [here](https://docs.aws.amazon.com/cdk/latest/guide/work-with.html#work-with-prerequisites).

## Running

```
astro dev start --env astro.env
```

## Development

### Airflow

#### Debugging a DAG

See [here](https://airflow.apache.org/docs/apache-airflow/stable/executor/debug.html) and
use VSCode launch configuration `"Python: Airflow DAG"` in `.vscode/launch.json`.

#### Deleting a DAG

```
airflow dags delete dag_id
```

### Environement variables

Set the following in `.env`:

- `NEWS_API_TOKEN`: API token from https://newsapi.org; Airflow variable name: `news_api_key`
