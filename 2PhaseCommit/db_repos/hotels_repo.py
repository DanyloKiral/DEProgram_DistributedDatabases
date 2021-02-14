import string
import random

import psycopg2
from psycopg2._psycopg import connection


class HotelsRepo:
    def __init__(self):
        self.connection: connection = psycopg2.connect(host="localhost",
                                                       database="2PC_DB2",
                                                       user="danylokiral",
                                                       password="")

        with self.connection.cursor() as cursor:
            cursor.execute("""create schema if not exists hotels;
                            create table if not exists hotels.hotel_bookings (
                                booking_id serial primary key,
                                client_name varchar(255),
                                hotel_name varchar(100),
                                arrival date,
                                departure date
                              );
                            """)
            cursor.close()

        self.connection.commit()

    def insert(self, client_name, hotel_name, arrival, departure):
        cursor = self.connection.cursor()
        cursor.execute("""
            insert into hotels.hotel_bookings (client_name, hotel_name, arrival, departure) 
            values (%s, %s, %s, %s)""",
                       (client_name, hotel_name, arrival, departure))
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

    def get_random_string(self, len_param=10):
        letters = string.ascii_letters
        result_str = ''.join(random.choice(letters) for i in range(len_param))
        return result_str

