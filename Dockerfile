FROM python:3.11-slim

# Install dbt-core and dbt-trino
RUN pip install --no-cache-dir \
    dbt-core \
    dbt-trino

# Set the working directory inside the container
WORKDIR /dbt

# Copy the dbt project files to the container
COPY ./dbt /dbt

# Default entrypoint to run dbt build
ENTRYPOINT ["dbt", "build", "--target", "trino", "--profiles-dir", "."]
