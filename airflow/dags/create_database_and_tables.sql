CREATE DATABASE IF NOT EXISTS health_metrics DEFAULT CHARSET = 'utf8mb4';
USE health_metrics;

CREATE TABLE IF NOT EXISTS users (
    user_id INT AUTO_INCREMENT PRIMARY KEY,
    first_name NVARCHAR(50),
    last_name NVARCHAR(50),
    age INT,
    gender ENUM('male', 'female', 'others'),
    email NVARCHAR(100)
);

CREATE TABLE IF NOT EXISTS metrics (
    user_id INT NOT NULL,
    date_time DATETIME NOT NULL,
    heart_rate INT,
    blood_oxygen FLOAT(7,2),
    blood_pressure FLOAT(7,2),
    steps_count INT,
    calories_burned FLOAT(7,2),
    body_temperature FLOAT(7,2),
);


DROP PROCEDURE IF EXISTS agg_metrics;

CREATE PROCEDURE agg_metrics()
BEGIN
    WITH metrics_by_user_daily AS (
        SELECT
            user_id,
            DATE(date_time) AS `date`,
            AVG(heart_rate) AS avg_heart_rate,
            AVG(blood_oxygen) AS avg_blood_oxygen,
            SUM(steps_count) AS total_steps_count,
            SUM(calories_burned) AS total_calories_burned,
            AVG(body_temperature) AS avg_body_temperature
        FROM health_metrics.metrics
        WHERE DATEDIFF(CURDATE() - INTERVAL 1 DAY, DATE(date_time)) <= 1 
        GROUP BY user_id, DATE(date_time)
    ), metrics_day_on_day AS (
        SELECT 
            *,
            LEAD(`date`) OVER(PARTITION BY user_id ORDER BY `date` DESC) AS date_prev,
            LEAD(avg_heart_rate) OVER(PARTITION BY user_id ORDER BY `date` DESC) AS avg_heart_rate_prev,
            LEAD(avg_blood_oxygen) OVER(PARTITION BY user_id ORDER BY `date` DESC) AS avg_blood_oxygen_prev,
            LEAD(total_steps_count) OVER(PARTITION BY user_id ORDER BY `date` DESC) AS total_steps_count_prev,
            LEAD(total_calories_burned) OVER(PARTITION BY user_id ORDER BY `date` DESC) AS total_calories_burned_prev,
            LEAD(avg_body_temperature) OVER(PARTITION BY user_id ORDER BY `date` DESC) AS avg_body_temperature_prev,
        FROM metrics_by_user_daily
    ), metrics_latest AS (
        SELECT 
            user_id,
            `date`,
            avg_heart_rate,
            (avg_heart_rate - avg_heart_rate_prev) / avg_heart_rate_prev * 100 AS avg_heart_rate_percent_change,
            avg_blood_oxygen,
            (avg_blood_oxygen - avg_blood_oxygen_prev) / avg_blood_oxygen_prev * 100 AS avg_blood_oxygen_percent_change,
            total_steps_count,
            (total_steps_count - total_steps_count_prev) / total_steps_count_prev * 100 AS total_steps_count_percent_change,
            total_calories_burned,
            (total_calories_burned - total_calories_burned_prev) / total_calories_burned_prev * 100 AS total_calories_burned_percent_change,
            avg_body_temperature,
            (avg_body_temperature - avg_body_temperature_prev) / avg_body_temperature_prev * 100 AS avg_body_temperature_percent_change,
        FROM metrics_day_on_day
        WHERE `date` = (SELECT MAX(DATE(date_time)) FROM health_metrics.metrics)
    )
    SELECT * FROM metrics_latest LEFT JOIN health_metrics.users ON metrics_latest.user_id = users.user_id;
END 




