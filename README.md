# Health Metrics DAG Project

Welcome to the **Health Metrics DAG** project! This repository demonstrates the use of Apache Airflow for generating synthetic health data, storing it in a MySQL database, and sending personalized daily health metric summaries via email.

---

## Table of Contents
- [Overview](#overview)
- [Features](#features)
- [Technologies Used](#technologies-used)
- [Setup](#setup)
- [How It Works](#how-it-works)
- [DAG Structure](#dag-structure)
- [Future Enhancements](#future-enhancements)
- [License](#license)

---

## Overview

This project leverages Airflow to automate the following tasks:
1. Generate synthetic user and health metric data using the `Faker` library.
2. Insert this data into a MySQL database with pre-defined tables.
3. Aggregate the daily health metrics for each user.
4. Email each user a summary of their health metrics.

---

## Features

- **Synthetic Data Generation**: Creates realistic user profiles and health metrics.
- **Database Automation**: Manages MySQL database creation, table initialization, and data insertion.
- **Email Automation**: Sends dynamic, HTML-formatted health metric summaries to users.
- **Scheduled Workflow**: Automates the entire pipeline daily using Apache Airflow.

---

## Technologies Used

- **Apache Airflow**: Orchestration of tasks and DAG scheduling.
- **Python**: Core language for task scripting.
- **MySQL**: Database for storing synthetic user and health data.
- **Faker**: Library for generating synthetic data.
- **SMTP**: Email automation using Gmail's SMTP server.

---

## Setup

1. **Clone this repository**:
   ```bash
   git clone https://github.com/your_username/health-metrics-dag.git
   cd health-metrics-dag
   ```

2. **Install Dependencies**:
   ```bash
   pip install apache-airflow mysql-connector-python faker
   ```

3. **Configure MySQL**:
   - Start MySQL:
     ```bash
     sudo service mysql start
     ```
   - Set up a MySQL user with the following credentials:
     ```
     Username: admin
     Password: 1234
     ```

4. **Set up Airflow**:
   - Initialize Airflow database:
     ```bash
     airflow db init
     ```
   - Create an Airflow connection for MySQL (`mysql_u_admin`) in the Airflow UI.

5. **Customize Configuration**:
   - Update the `sample_name` and `sample_email` variables in the script with your details.
   - Update the SMTP credentials for email functionality.

6. **Run the DAG**:
   - Start the Airflow scheduler and web server:
     ```bash
     airflow scheduler &
     airflow webserver &
     ```
   - Trigger the DAG from the Airflow UI.

---

## How It Works

### 1. Data Generation
The `generate_users` and `generate_metrics` Python functions create realistic user profiles and health metric data using the `Faker` library.

### 2. Database Management
The MySQL tasks initialize the database and insert the generated data.

### 3. Email Notifications
The `agg_metrics_and_send_mail` function aggregates daily metrics and sends a personalized HTML email summary.

---

## DAG Structure

- **`start_mysql`**: Starts the MySQL server.
- **`create_database_and_tables`**: Creates the database and necessary tables.
- **`generate_users`**: Generates synthetic users.
- **`generate_metrics`**: Generates synthetic health metrics.
- **`send_emails`**: Aggregates metrics and sends email notifications.

### Workflow Graph
```plaintext
start_mysql >> create_database_and_tables >> [generate_users, generate_metrics] >> send_emails
```

---

## Future Enhancements

- Add support for more complex health metrics.
- Implement user authentication for secure email delivery.
- Enhance visualization of data with dashboards.
- Migrate to a cloud database for scalability.

---

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

---


