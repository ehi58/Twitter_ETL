# Twitter ETL Pipeline Using Apache Airflow and Postgres

This project implements an ETL (Extract, Transform, Load) pipeline using Apache Airflow to gather and process Twitter data. The script `airflow_twitter_etl.py` schedules, orchestrates, and automates the ETL process to extract Twitter data, transform it, and load it into a designated data store.

## Project Structure

- **`airflow_twitter_etl.py`**: The main Airflow DAG script that defines the ETL pipeline for Twitter data. It handles the scheduling and orchestration of the pipeline tasks.
- **Data sources**: This ETL pipeline uses Twitter's API to fetch data.
- **Transformations**: Data is cleaned and transformed before being loaded into the target storage.
- **Storage**: Data can be loaded into various destinations, such as AWS S3, a relational database, or a data warehouse.

## Features

- **Airflow DAGs**: This project uses Directed Acyclic Graphs (DAGs) in Airflow to manage ETL workflows.
- **Modular design**: The ETL tasks are modularized into extract, transform, and load components.
- **Scheduled Execution**: Pipelines can be scheduled to run at specific intervals, enabling the automation of the data collection and processing pipeline.
- **Twitter API Integration**: Uses the Twitter API to fetch real-time data for analysis.

## Installation

1. **Install Apache Airflow**: Follow [Apache Airflow's official documentation](https://airflow.apache.org/docs/apache-airflow/stable/start.html) for installation.
2. **Clone the repository**:
    ```bash
    git clone https://github.com/your-username/twitter-etl-airflow.git
    cd twitter-etl-airflow
    ```
3. **Install dependencies**: 
   - Install the required Python libraries using the `requirements.txt` file:
    ```bash
    pip install -r requirements.txt
    ```

4. **Configure Twitter API**: Set up your Twitter API keys by configuring the environment variables or using a credentials file:
    - `TWITTER_API_KEY`
    - `TWITTER_API_SECRET_KEY`
    - `TWITTER_ACCESS_TOKEN`
    - `TWITTER_ACCESS_TOKEN_SECRET`

5. **Set up Airflow**: Ensure your Airflow environment is configured correctly with the proper `airflow.cfg` file.

## Usage

1. **Start Airflow**:
    ```bash
    airflow webserver --port 8080
    airflow scheduler
    ```

2. **Trigger the DAG**: Once Airflow is up and running, you can trigger the Twitter ETL DAG through the Airflow UI or CLI.

3. **Monitor DAG**: Monitor the status of tasks in the DAG using the Airflow web UI.

## Customization

- You can modify the frequency of the DAG execution by adjusting the schedule interval in the `airflow_twitter_etl.py` file.
- The script can be adapted to load data into different destinations (e.g., AWS S3, a database).
- Modify the data extraction or transformation logic based on your specific use case or data requirements.

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.

## Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.
