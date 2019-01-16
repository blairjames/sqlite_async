#!/usr/bin/env python3

from sqlite3 import connect
from typing import List
from asyncio import get_event_loop


class SqliteAsync:

    def __init__(self):
        '''
        Instantiation creates in-memory database in the class scope
        '''
        try:
            with connect(':memory:') as self.memdb:
                self.memdb.text_factory = str
                self.memcur = self.memdb.cursor()
                self.memexe = self.memcur.execute
                self.memexecutemany = self.memcur.executemany
                self.memcommit = self.memdb.commit
                self.memroll = self.memdb.rollback
        except Exception as e:
            self.exceptor("Error! in constructor", e)


    async def create_in_memory_table(self, table_name: str, attributes: str):
        '''
        Creates table on the class scope in-memory database
        '''
        try:
            self.memtable = table_name
            self.attributes = attributes
            create = "create table " + table_name + "(" + attributes +")"
            self.memexe(create)
            self.memcommit()
        except Exception as e:
            self.exceptor("Error! in create_inmemory_database", e)


    async def read_diskdb_into_memdb(self, diskdb_name: str, disktable: str):
        '''
        Select records from disk database and insert into in-memory database.
        '''
        try:
            print("Reading db into ram.")
            attachDatabaseSQL = "ATTACH DATABASE ? AS diskdb"
            db = (diskdb_name,)
            self.memexe(attachDatabaseSQL, db)
            self.memcommit()
            up = 50000
            low = 0
            while up < 20000000:
                disktablesql = "select * from diskdb." + disktable + \
                               " where rowid < " + str(up) \
                               + " and rowid > " + str(low)
                [self.memdb.cursor().execute("insert into " + self.memtable +
                                             " values (?, ?)", tuple(x)) for x in self.memexe(disktablesql)]
                self.memcommit()
                up += 50000
                low += 50000
        except Exception as e:
            self.memroll()
            self.exceptor("Error! in read_diskdb_into_memdb", e)


    async def create_index_diskdb(self, dbname: str, index_name: str, table_name: str, index_column: str):
        '''
        Create index on given table and attribute
        '''
        try:
            print("indexing...")
            with connect(dbname) as diskdb:
                diskdb.cursor().execute("CREATE INDEX if not exists " + index_name
                                        + " on " + table_name + "(" + index_column + ")")
                diskdb.commit()
                print("complete.")
        except Exception as e:
            self.exceptor("Error! in create_index", e)


    async def select_from_inmem_db(self, table: str, column: str, search: str):
        '''
        Retreive records from in-memory database using specified table and predicate
        '''
        try:
            statement = str("SELECT * FROM " + table + " where " + column + " like ?;")
            param = (search, )
            print(statement.rstrip("?;") + search)
            stat = self.memexe(statement, param)
            results = stat.fetchall()
            print(results)
            self.memcommit()
            return [x for x in results]

        except Exception as e:
            self.exceptor("Error! in select_from_inmem_db", e)


    async def select_from_disk_db(self, dbname: str, table: str, column: str, search: str):
        '''
        Retreive records from specified table and predicate
        '''
        try:
            with connect(dbname) as diskdb:
                diskdb.text_factory = str
                diskdb.cursor().execute("create index if not exists myindex on " + table + "(" + column + ");")
                print("index complete")
                statement = str("SELECT " + column + " FROM " + table + " where " + column + " like ?;")
                param = (search, )
                print(statement.rstrip("?;") + search)
                diskdb.text_factory = str
                results = [x for x in diskdb.cursor().execute(statement, param) for x in x]
                diskdb.commit()
                return results
        except Exception as e:
            self.exceptor("Error! in select_from_disk_db", e)


    async def tuppler(self, sequence: List):
        '''
        Convert List to Dict to Tupple for SQL INSERT statement
        Returns list of tupples
        '''
        try:
            dick = {sequence[i]: sequence[i+1] for i in range(0, len(sequence), 2)}
            list_of_tupples = list(tuple(dick.items()))
            return [d for d in list_of_tupples]
        except Exception as e:
            self.exceptor("Error! in tuppler", e)


    async def writer(self, dbname: str, table: str, values_as_list_of_tuples: List):
        '''
        Writes to disk database with bulk insert statements, using a list of tupples.
        '''
        try:
            print("Writing to Database: " + dbname)
            with connect(dbname) as diskdb:
                diskdb.cursor().executemany("insert into " + table + " values (?, ?)", values_as_list_of_tuples)
                print("write complete.")
        except Exception as e:
            self.exceptor("Error in Writer!", e)


    def exceptor(self, message: str, exception: Exception):
        '''
        Exception handler for Class.
        '''
        try:
            self.memroll()
            mes = message + ": " + str(exception)
            print(mes)
            exit(1)
        except Exception as e:
            print("Error! in Exceptor: " + str(e))
            exit(1)


    async def controller(self):
        '''
        Controller method to drive execution, called by the async loop to co-ordinate awaits
        '''
        try:
            pass
        except Exception as e:
            self.exceptor("Error in controller!", e)


def main():
    loop = get_event_loop()
    loop.run_until_complete(loop.create_task(SqliteAsync().controller()))


if __name__ == '__main__':
    main()
