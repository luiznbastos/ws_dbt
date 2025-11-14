FROM python:3.11-slim

WORKDIR /app

# Install dbt-postgres
RUN pip install --no-cache-dir dbt-postgres

# Copy dbt project
COPY src/ ./src/

# Run dbt
WORKDIR /app/src
CMD ["dbt", "run"]
