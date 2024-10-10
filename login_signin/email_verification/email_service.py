from flask import jsonify
import sendgrid
from sendgrid.helpers.mail import Mail
from fetch_secret import access_secret_version
from config.settings import Config
from random import randint


def send_verification_email(to_email):
    """Send a verification email with a 4-digit code."""
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
        if response.status_code == 202:
            jsonify(f"An email has been sent to {to_email} with a verification code.")
            return True, code  # Return True if email is sent successfully, and the code
        else:
            return False, None
    except Exception as e:
        return False, None
