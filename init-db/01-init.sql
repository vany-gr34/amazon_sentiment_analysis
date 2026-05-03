-- Create databases
CREATE DATABASE airflow;
CREATE DATABASE metabase;
CREATE DATABASE mlflow;

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE airflow TO mluser;
GRANT ALL PRIVILEGES ON DATABASE metabase TO mluser;
GRANT ALL PRIVILEGES ON DATABASE mlflow TO mluser;
