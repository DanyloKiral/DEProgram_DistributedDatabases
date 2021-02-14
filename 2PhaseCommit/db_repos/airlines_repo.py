import string
import random

import psycopg2
from psycopg2._psycopg import connection


class AirlinesRepo:
    def __init__(self):
        self.connection: connection = psycopg2.connect(host="localhost",
                                                       database="2PC_DB1",
                                                       user="danylokiral",
                                                       password="")

        with self.connection.cursor() as cursor:
            cursor.execute("""create schema if not exists airlines;
                            create table if not exists airlines.flights_bookings (
                                booking_id serial primary key,
                                client_name varchar(255),
                                flight_number varchar(20),
                                from_airport varchar(3),
                                to_airport varchar(3),
                                flight_date date
                              );
                            """)
            cursor.close()

        self.connection.commit()

    def insert(self, client_name, fly_number, from_airport, to_airport, fly_date):
        cursor = self.connection.cursor()
        cursor.execute("""
            insert into airlines.flights_bookings (client_name, flight_number, from_airport, to_airport, flight_date) 
            values (%s, %s, %s, %s, %s)""",
                       (client_name, fly_number, from_airport, to_airport, fly_date))
        cursor.close()

    def start_transaction(self):
        xid = self.connection.xid(random.randint(1, 10000), self.get_random_string(), self.get_random_string())
        self.connection.tpc_begin(xid)
        return xid

    def prepare(self):
        self.connection.tpc_prepare()

    def rollback(self):
        self.connection.tpc_rollback()

    def commit(self):
        self.connection.tpc_commit()

    def get_random_string(self, len_param = 10):
        letters = string.ascii_letters
        result_str = ''.join(random.choice(letters) for i in range(len_param))
        return result_str


