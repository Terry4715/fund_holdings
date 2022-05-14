from psycopg2 import pool


class Database:
    # __ at the front of a variable name makes it inaccessible outside a class
    __connection_pool = None

    # initialising the connection to our database inside a method of a class
    # allows us to choose when this connection is executed
    @staticmethod
    def initialise(**kwargs):
        Database.__connection_pool = pool.SimpleConnectionPool(minconn=1,
                                                               maxconn=8,
                                                               **kwargs)

    # gets a connection from the pool of connections created when initialising
    @classmethod
    def get_connection(cls):
        return cls.__connection_pool.getconn()

    # returns a connection used back to the connection pool
    @classmethod
    def return_connection(cls, connection):
        cls.__connection_pool.putconn(connection)

    # stops the commit from going through to any connection and closes them all
    @classmethod
    def close_all_connections(cls):
        Database.__connection_pool.closeall()


class CursorFromPool:
    def __init__(self):
        self.connection = None
        self.cursor = None

    # __enter___ & __exit__ allows a with statement to be used
    # to automatically close cursor and database connections
    def __enter__(self):
        self.connection = Database.get_connection()
        self.cursor = self.connection.cursor()
        return self.cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_val is not None:    # e.g. TypeError, AttributeError, ValueError
            self.connection.rollback()
            # prints the error fed back from a SQL query if there is an issue
            print(f"Error from SQL query: {exc_type} | {exc_val} | {exc_tb}")
        else:
            self.cursor.close()
            self.connection.commit()
        Database.return_connection(self.connection)
