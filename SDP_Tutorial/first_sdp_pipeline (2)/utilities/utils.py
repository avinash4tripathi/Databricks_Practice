from pyspark.sql.functions import col

def is_valid_email(email_col):
    return email_col.rlike(r"^[^@]+@[^@]+\.[^@]+$")
