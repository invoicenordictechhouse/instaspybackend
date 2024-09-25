# Use the official dbt image from dbt Labs (previously Fishtown Analytics)
FROM fishtownanalytics/dbt:latest

# Set the working directory inside the container
WORKDIR /dbt

# Copy the dbt project files into the container
COPY . /dbt

# Install any additional Python dependencies (if you have a requirements.txt)
RUN pip install -r requirements.txt

# When the container starts, run the dbt command
CMD ["dbt", "run"]
# Comment: This Dockerfile builds a dbt environment and runs the 'dbt run' command to execute all models.
