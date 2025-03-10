import re


def is_valid_email(email):
    """Check if an email is valid."""
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(email_pattern, email))

def is_valid_phone(phone):
    """Validate phone number format."""
    phone = str(phone)
    phone_pattern = r'^\+?[0-9\s\-\(\)]{7,}$'
    return bool(re.match(phone_pattern, phone))

def is_valid_number(value):
    """Check if the number is valid (non-negative)."""
    value = float(value)
    return value >= 0