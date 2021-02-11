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
            INSERT INTO hotels.hotel_bookings (client_name, hotel_name, arrival, departure) 
            VALUES(%s, %s, %s, %s, %s)""",
                       (client_name, hotel_name, arrival, departure))
        cursor.close()

    def start_transaction(self):
        xid = self.connection.xid(1, 'transaction ID', 'connection1')
        self.connection.tpc_begin(xid)
        return xid

    def prepare(self):
        self.connection.tpc_prepare()

    def rollback(self, xid):
        self.connection.tpc_rollback(xid)

    def commit(self, xid):
        self.connection.tpc_commit(xid)

