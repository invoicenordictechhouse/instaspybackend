from config.settings import Config

# Query to get verification code, hashed password, and created time
GET_VERIFICATION_CODE = """
    SELECT verification_code, created_at
    FROM `annular-net-436607-t0.Instaspy_DS.verification_codes`
    WHERE email = @email
    LIMIT 1;
"""

GET_USER = f"""
    SELECT * FROM `{Config.PROJECT_ID}.{Config.DATASET_ID}.{Config.TABLE_ID}`
    WHERE email = @email
    """

DELETE_USER_FROM_TEMP_TABLE = """
        DELETE FROM `annular-net-436607-t0.Instaspy_DS.verification_codes`
        WHERE email = @email
    """
