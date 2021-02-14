import sys
from datetime import date
from psycopg2._psycopg import DatabaseError
from db_repos.accounts_repo import AccountsRepo
from db_repos.airlines_repo import AirlinesRepo
from db_repos.hotels_repo import HotelsRepo


class Main():
    def __init__(self):
        self.airlines_repo: AirlinesRepo = AirlinesRepo()
        self.hotels_repo: HotelsRepo = HotelsRepo()
        self.accounts_repo: AccountsRepo = AccountsRepo()

    def main(self):
        self.book_payed_trip_via_2pc({
            'client_name': 'Danylo Kiral',
            'departure_flight_number': 'PS 1212',
            'arrival_flight_number': 'WZ 2323',
            'home_airport': 'LWO',
            'destination_airport': 'AMS',
            'arrival_date': date(2021, 1, 1),
            'departure_date': date(2021, 2, 2),
            'hotel_name': 'hotel1',
            'price': 1000
        })

    def book_payed_trip_via_2pc(self, trip_data: dict):
        self.airlines_repo.start_transaction()
        self.hotels_repo.start_transaction()
        self.accounts_repo.start_transaction()

        self.airlines_repo.insert(
            trip_data['client_name'],
            trip_data['departure_flight_number'],
            trip_data['home_airport'],
            trip_data['destination_airport'],
            trip_data['departure_date']
        )

        self.hotels_repo.insert(
            trip_data['client_name'],
            trip_data['hotel_name'],
            trip_data['arrival_date'],
            trip_data['departure_date']
        )

        self.airlines_repo.insert(
            trip_data['client_name'],
            trip_data['arrival_flight_number'],
            trip_data['destination_airport'],
            trip_data['home_airport'],
            trip_data['arrival_date']
        )

        self.accounts_repo.decrease_account_amount(
            trip_data['client_name'],
            trip_data['price']
        )

        try:
            self.airlines_repo.prepare()
            self.hotels_repo.prepare()
            self.accounts_repo.prepare()
        except DatabaseError:
            print(sys.exc_info())
            self.airlines_repo.rollback()
            self.hotels_repo.rollback()
            self.accounts_repo.rollback()
        else:
            self.airlines_repo.commit()
            self.hotels_repo.commit()
            self.accounts_repo.commit()

    def book_trip_via_2pc(self, trip_data: dict):
        self.airlines_repo.start_transaction()
        self.hotels_repo.start_transaction()

        self.airlines_repo.insert(
            trip_data['client_name'],
            trip_data['departure_flight_number'],
            trip_data['home_airport'],
            trip_data['destination_airport'],
            trip_data['departure_date']
        )

        self.hotels_repo.insert(
            trip_data['client_name'],
            trip_data['hotel_name'],
            trip_data['departure_date'],
            trip_data['arrival_date']
        )

        self.airlines_repo.insert(
            trip_data['client_name'],
            trip_data['arrival_flight_number'],
            trip_data['destination_airport'],
            trip_data['home_airport'],
            trip_data['arrival_date']
        )

        try:
            self.airlines_repo.prepare()
            self.hotels_repo.prepare()
        except DatabaseError:
            print(sys.exc_info())
            self.airlines_repo.rollback()
            self.hotels_repo.rollback()
        else:
            self.airlines_repo.commit()
            self.hotels_repo.commit()


if __name__ == '__main__':
    Main().main()

