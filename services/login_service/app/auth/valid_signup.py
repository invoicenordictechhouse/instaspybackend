import re


def is_valid_email(email: str) -> bool:
    """
    Validate the email format.

    Args:
        email (str): The user's email to validate.

    Returns:
        bool: True if the email is valid, False otherwise.
    """
    email_regex = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
    return re.match(email_regex, email) is not None


# Password validation function
def is_valid_password(password: str) -> bool:
    """
    Validate the password strength.

    Args:
        password (str): The user's password to validate.

    Returns:
        bool: True if the password meets requirements, False otherwise.
    """
    if len(password) < 7:
        return False
    if not re.search(r"[a-zA-Z]", password):  # Check if password contains a letter
        return False
    if not re.search(r"\d", password):  # Check if password contains a number
        return False
    return True
