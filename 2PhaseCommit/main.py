import sys
from datetime import date
from psycopg2._psycopg import DatabaseError
from db_repos.accounts_repo import AccountsRepo
from db_repos.airlines_repo import AirlinesRepo
from db_repos.hotels_repo import HotelsRepo


class Main():
    def __init__(self):
        self.client_name = 'Danylo Kiral'
        self.account_amount = 1700

        self.airlines_repo: AirlinesRepo = AirlinesRepo()
        self.hotels_repo: HotelsRepo = HotelsRepo()
        self.accounts_repo: AccountsRepo = AccountsRepo(self.client_name, self.account_amount)

    def main(self):
        self.book_payed_trip_via_2pc({
            'client_name': self.client_name,
            'departure_flight_number': 'PS 1212',
            'arrival_flight_number': 'WZ 2323',
            'home_airport': 'LWO',
            'destination_airport': 'AMS',
            'arrival_date': date(2021, 3, 2),
            'departure_date': date(2021, 3, 15),
            'hotel_name': 'Amsterdam hotel',
            'price': 1000
        })

        self.book_payed_trip_via_2pc({
            'client_name': self.client_name,
            'departure_flight_number': 'PS 4545',
            'arrival_flight_number': 'PS 2343',
            'home_airport': 'LWO',
            'destination_airport': 'ATH',
            'arrival_date': date(2021, 6, 7),
            'departure_date': date(2021, 6, 30),
            'hotel_name': 'Athens hotel',
            'price': 500
        })

        # third trip booking should fail (as account amount at this point is 200)
        # no records should be inserted
        self.book_payed_trip_via_2pc({
            'client_name': self.client_name,
            'departure_flight_number': 'RN 7674',
            'arrival_flight_number': 'RN 2343',
            'home_airport': 'LWO',
            'destination_airport': 'OSL',
            'arrival_date': date(2021, 10, 5),
            'departure_date': date(2021, 10, 10),
            'hotel_name': 'Oslo hotel',
            'price': 500
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

