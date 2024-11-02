import subprocess
import logging

from fastapi import HTTPException


def run_dbt_command(command: str):
    """
    Function to run a DBT command using subprocess.
    """
    try:
        result = subprocess.run(
            command, shell=True, check=True, capture_output=True, text=True
        )
        if result.returncode != 0:
            error_message = result.stderr.strip()
            logging.error(f"DBT command failed with error: {error_message}")
            
            raise HTTPException(status_code=500, detail=f"DBT command failed: {error_message}")

        logging.info(f"DBT command - {result.stdout} completed successfully.")

    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to execute dbt command: {e}")
        raise HTTPException(status_code=500, detail="An unexpected error occurred while executing the dbt command.")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        raise HTTPException(status_code=500, detail="An unexpected error occurred.")
