import subprocess
import logging


def run_dbt_command(command: str):
    """
    Function to run a DBT command using subprocess.
    """
    try:
        result = subprocess.run(
            command, shell=True, check=True, capture_output=True, text=True
        )
        logging.info(f"DBT command executed successfully: {result.stdout}")
    except subprocess.CalledProcessError as e:
        logging.error(f"DBT command failed: {e.stderr}")
        raise
