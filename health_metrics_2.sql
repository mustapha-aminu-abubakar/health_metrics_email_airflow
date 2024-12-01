CREATE DATABASE IF NOT EXISTS health_metrics_2 DEFAULT CHARSET = 'utf8mb4';
USE health_metrics_2;

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
    steps_count INT,
    calories_burned FLOAT(7,2),
    sleep_duration FLOAT(7,2),
    stress_level INT,
    body_temperature FLOAT(7,2),
    activity_level FLOAT(7,2),
    PRIMARY KEY (user_id, date_time),
    Foreign Key (user_id) REFERENCES health_metrics_2.users(user_id)
);


DELIMITER $$

DROP PROCEDURE IF EXISTS agg_metrics$$

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
            AVG(stress_level) AS avg_stress_level,
            AVG(body_temperature) AS avg_body_temperature,
            CASE 
                WHEN AVG(activity_level) < 0.5 THEN 'low'
                WHEN AVG(activity_level) < 0.8 THEN 'moderate'
                ELSE 'high'
            END AS avg_activity_level
        FROM health_metrics_2.metrics
        WHERE DATEDIFF(NOW(), date_time) <= 2
        GROUP BY user_id, DATE(date_time)
    ), metrics_day_on_day AS (
        SELECT 
            *,
            LEAD(avg_heart_rate) OVER(PARTITION BY user_id ORDER BY `date` DESC) AS avg_heart_rate_prev,
            LEAD(avg_blood_oxygen) OVER(PARTITION BY user_id ORDER BY `date` DESC) AS avg_blood_oxygen_prev,
            LEAD(total_steps_count) OVER(PARTITION BY user_id ORDER BY `date` DESC) AS total_steps_count_prev,
            LEAD(total_calories_burned) OVER(PARTITION BY user_id ORDER BY `date` DESC) AS total_calories_burned_prev,
            LEAD(avg_stress_level) OVER(PARTITION BY user_id ORDER BY `date` DESC) AS avg_stress_level_prev,
            LEAD(avg_body_temperature) OVER(PARTITION BY user_id ORDER BY `date` DESC) AS avg_body_temperature_prev,
            LEAD(avg_activity_level) OVER(PARTITION BY user_id ORDER BY `date` DESC) AS avg_activity_level_prev
        FROM metrics_by_user_daily
    )
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
        avg_stress_level,
        (avg_stress_level - avg_stress_level_prev) / avg_stress_level_prev * 100 AS avg_stress_level_percent_change,
        avg_body_temperature,
        (avg_body_temperature - avg_body_temperature_prev) / avg_body_temperature_prev * 100 AS avg_body_temperature_percent_change,
        avg_activity_level,
        CASE 
            WHEN avg_activity_level = avg_activity_level_prev THEN 'no change'
            WHEN avg_activity_level = 'high' AND avg_activity_level_prev != 'high' THEN 'increased'
            WHEN avg_activity_level = 'low' AND avg_activity_level_prev != 'low' THEN 'decreased'
            WHEN avg_activity_level = 'moderate' AND avg_activity_level_prev = 'high' THEN 'decreased'
            WHEN avg_activity_level = 'moderate' AND avg_activity_level_prev != 'low' THEN 'increased'
        END AS avg_activity_level_change
    FROM metrics_day_on_day
    WHERE `date` = (SELECT MAX(DATE(date_time)) FROM health_metrics_2.metrics);
END $$

DELIMITER ;



