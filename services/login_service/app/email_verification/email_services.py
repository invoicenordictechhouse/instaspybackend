from fastapi import HTTPException
import sendgrid
from sendgrid.helpers.mail import Mail
from fetch_secret import access_secret_version
from config.settings import Config
from random import randint


def send_verification_email(to_email: str):
    """
    Send a verification email with a 4-digit code.

    Args:
        to_email (str): Recipient's email address.

    Returns:
        Tuple[bool, str]: A tuple with a boolean indicating success and the verification code.
    """
    subject = "Verify Your Email"
    code = f"{randint(1000, 9999)}"  # Generate a 4-digit verification code
    content = f"Please use the following code to verify your email address:\n{code}"

    sg = sendgrid.SendGridAPIClient(
        api_key=access_secret_version(Config.PROJECT_ID, "sendgrid_email_api")
    )
    from_email = "developer@heroecom.com"

    mail = Mail(from_email, to_email, subject, content)

    try:
        response = sg.send(mail)
        if response.status_code == 202:  # Email sent successfully
            return True, code  # Return True and the verification code
        else:
            return False, None  # Failed to send email
    except Exception:
        raise HTTPException(status_code=500, detail="Failed to send verification email")
