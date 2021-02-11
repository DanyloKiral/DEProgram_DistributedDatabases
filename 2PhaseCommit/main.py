import sys
from datetime import date

from psycopg2._psycopg import DatabaseError

from db_repos.airlines_repo import AirlinesRepo


def main():
    airlines_repo = AirlinesRepo()
    airlines_repo.start_transaction()

    airlines_repo.insert('client', 'FLY 5462', 'LWO', 'AMS', date(2020, 12, 5))

    try:
        airlines_repo.prepare()
    except DatabaseError:
        r = sys.exc_info()
        airlines_repo.rollback()
    else:
        airlines_repo.commit()


if __name__ == '__main__':
    main()

