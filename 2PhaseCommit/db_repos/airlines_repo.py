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
                            create table if not exists airlines.fly_bookings (
                                booking_id serial primary key,
                                client_name varchar(255),
                                fly_number varchar(20),
                                from_airport varchar(3),
                                to_airport varchar(3),
                                fly_date date
                              );
                            """)
            cursor.close()

        self.connection.commit()

    def insert(self, client_name, fly_number, from_airport, to_airport, fly_date):
        cursor = self.connection.cursor()
        cursor.execute("""
            INSERT INTO airlines.fly_bookings (client_name, fly_number, from_airport, to_airport, fly_date) 
            VALUES(%s, %s, %s, %s, %s)""",
                       (client_name, fly_number, from_airport, to_airport, fly_date))
        cursor.close()

    def start_transaction(self):
        xid = self.connection.xid(5, 'transaction ID3', 'connection1')
        self.connection.tpc_begin(xid)
        return xid

    def prepare(self):
        self.connection.tpc_prepare()

    def rollback(self):
        self.connection.tpc_rollback()

    def commit(self):
        self.connection.tpc_commit()


