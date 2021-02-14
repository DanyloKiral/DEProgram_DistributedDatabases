import random
import string

import psycopg2
from psycopg2._psycopg import connection


class AccountsRepo:
    def __init__(self):
        self.connection: connection = psycopg2.connect(host="localhost",
                                                       database="2PC_DB3",
                                                       user="danylokiral",
                                                       password="")

        with self.connection.cursor() as cursor:
            cursor.execute("""create schema if not exists banks;
                            create table if not exists banks.accounts (
                                account_id serial primary key,
                                client_name varchar(255),
                                amount NUMERIC(11,2) CHECK (amount >= 0),
                                unique(client_name)
                              );
                            insert into banks.accounts (client_name, amount)
                            values ('Danylo Kiral', 2000) 
                            ON CONFLICT DO NOTHING;
                            """)
            cursor.close()

        self.connection.commit()

    def decrease_account_amount(self, client_name, decrease_by):
        cursor = self.connection.cursor()
        cursor.execute("""
            do $$
            begin
                update banks.accounts set amount = amount - %s where client_name = %s;
                if not found then raise exception 'User not found';
                end if;
            end $$;""",
                       (decrease_by, client_name))
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