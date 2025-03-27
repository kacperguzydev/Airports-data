Airports Data
A data pipeline project for ingesting, storing, and visualizing airport-related data. This repository demonstrates how to:

Ingest data (e.g., flight information, airport details) via a producer script

Process or consume that data in near real-time

Store the data in a SQL database

Visualize insights through a Python-based dashboard

Features
Producer Script (producer.py): Sends airport data (from CSVs, APIs, or other sources) to a streaming platform or directly to the consumer.

Consumer Script (consumer.py): Receives the data, cleans/transforms it, and writes it to a database.

Database Setup (sql_database.py): Automates the creation of necessary tables in your SQL database.

Dashboard/Visualizer (visualizer.py): Provides an interactive view (e.g., via Streamlit or another framework) for exploring and analyzing the airport data.

Modular: Easy to extend to more complex data transformations or advanced analytics.
