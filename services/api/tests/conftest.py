import os

# A non-routable test URL permits application construction without contacting a database.
os.environ.setdefault("DATABASE_URL", "postgresql+psycopg://test_user:test_password@db.invalid:5432/govtracts_test")
