# Bike Station Monitor

## Introduction
The Bike Station Monitor project is designed to monitor bike station data by scraping, processing, and managing this data. The project uses AWS services and AWS CDK for infrastructure as code, ensuring scalable and efficient handling of bike station data.

## Table of Contents
- [Introduction](#introduction)
- [Project Structure](#project-structure)
- [Setup Instructions](#setup-instructions)
- [How It Works](#how-it-works)
- [References](#references)

## Project Structure
- **bike_data_scraper/**: Contains scripts for scraping bike data.
- **data/**: Directory for storing scraped data.
- **test/**: Contains test scripts to validate data scraping and processing.
- **database/**: Scripts for database setup and management.
- **events/**: Event handlers or triggers for data processing.
- **sample-lambda/**: Sample AWS Lambda functions used in the project.
- **infrastructure/**: Infrastructure setup files using AWS CDK.
- **tests/**: Unit tests for various components of the project.
- **app.py**: Main application script.
- **dev_app.py**: Development version of the application script.
- **cdk.json, cdk_json.txt**: AWS CDK configuration files.
- **Makefile**: Commands for setting up and managing the project environment.

## Setup Instructions

### Prerequisites
- **Python 3.10 or later**
- **AWS CLI**: Ensure that your AWS CLI is configured with the necessary credentials.
- **Node.js and npm**: Required for AWS CDK.

### Steps
1. **Clone the repository**:
    ```sh
    git clone https://github.com/eliaselhaddad/Bike-Station-Monitor-Internship1.git
    cd Bike-Station-Monitor-Internship1
    ```

2. **Set up the virtual environment** (optional but recommended):
    ```sh
    python -m venv env
    source env/bin/activate  # On Windows use `env\Scripts\activate`
    ```

3. **Install dependencies using Poetry**:
    ```sh
    pip install poetry
    poetry install
    ```

4. **Set up AWS CDK**:
    ```sh
    npm install -g aws-cdk
    cdk bootstrap
    ```

## How It Works
The Bike Station Monitor project scrapes data from various bike stations, processes it, and stores it for further analysis. The infrastructure is managed using AWS CDK, which allows for scalable deployment of the application. The main components include:
- **Data Scraper**: Collects data from bike stations.
- **Data Processing**: Cleans and processes the scraped data.
- **AWS Lambda Functions**: Handle event-driven data processing.
- **Infrastructure Management**: Uses AWS CDK to deploy and manage the application infrastructure.

## References
- [AWS CDK](https://docs.aws.amazon.com/cdk/latest/guide/home.html)
- [Poetry](https://python-poetry.org/docs/)
- [Boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)
- [Pandas](https://pandas.pydata.org/)
