"""Private database-connectivity verification command.

This module intentionally prints only the active database and database user,
never the connection URL or password.
"""

from sqlalchemy import text

from app.core.config import load_settings
from app.db.session import Database


def main() -> None:
    database = Database(load_settings())
    try:
        with database.engine.connect() as connection:
            row = connection.execute(text("SELECT current_database(), current_user")).one()
        print(f"Connected to PostgreSQL database '{row[0]}' as '{row[1]}'.")
    finally:
        database.dispose()


if __name__ == "__main__":
    main()
