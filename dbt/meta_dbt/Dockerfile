# Use the official dbt image from dbt Labs (previously Fishtown Analytics)
FROM fishtownanalytics/dbt:latest

# Set the working directory inside the container
WORKDIR /dbt

# Copy the dbt project files into the container
COPY . /dbt

# Install any additional Python dependencies (if you have a requirements.txt)
# Comment: This step is only needed if you have specific Python libraries to install.
# If not, you can omit the RUN command below.
RUN pip install -r requirements.txt || echo "No requirements.txt found, skipping."

# When the container starts, run the dbt command
CMD ["dbt", "run"]
# Comment: The Dockerfile builds a containerized environment that runs 'dbt run' for the Meta project.
