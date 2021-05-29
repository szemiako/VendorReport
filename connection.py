import pyodbc

'''Module to interact with any databases.'''

class BaseConnection:
    '''Base connection to database with methods.'''
    def __init__(
        self,
        server,
        database,
    ):
        self._server = server
        self._database = database
        self._driver = '{SQL Server}'
        self._trusted_connection = 'yes'
        self._connection_parameters = '''
            Driver={driver};
            Server={server};
            Database={db};
            Trusted_Connection={trusted_connection}
        '''.format(
            driver = self._driver,
            server = self._server,
            db = self._database,
            trusted_connection = self._trusted_connection
        )
        self._connection = pyodbc.connect(self._connection_parameters)
        self._cursor = self._open()

    def _open(self):
        '''Make a new cursor.'''
        return self._connection.cursor()

    def _close(self):
        '''Close the existing cursor.'''
        return self._cursor.close()

    def _new_cursor(self):
        '''"Refersh" (i.e., make a new) cursor.'''
        self._close()
        return self._open()

    def _execute(self, query):
        '''Execute a given query. Used for building temp tables and
        adding data to them. In this case, we don't care about the
        results of the query.'''
        self._cursor.execute(query)
        self._cursor = self._new_cursor()

    def _execute_select_all(self, query):
        '''Execute a query and return all results from the query.'''
        self._cursor.execute(query)
        results = self._cursor.fetchall()
        self._cursor = self._new_cursor()
        return results

class Connection(BaseConnection):
    '''Create a database connection using default production values.'''
    def __init__(
        self,
        server='${PRODUCTION}', # Default server.
        database='${DB}' # Default databse.
    ):
        super().__init__(
            server,
            database
        )