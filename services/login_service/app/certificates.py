import tempfile


def save_certificate_as_temp_file(cert_content: str) -> str:
    """
    Creates a temporary .crt file from the given certificate content and returns the file path.

    :param cert_content: The content of the certificate (as a string).
    :return: The full path to the created temporary .crt file.
    """
    try:
        # Create a temporary file with a .crt extension
        with tempfile.NamedTemporaryFile(delete=False, suffix=".crt") as temp_cert_file:
            # Write the certificate content to the file
            temp_cert_file.write(cert_content.encode("utf-8"))
            # Return the path to the temporary file
            return temp_cert_file.name
    except Exception as e:
        print(f"Error creating temporary .crt file: {e}")
        return None
