"""
Helper methods for converting to and from SQL row data to numpy array column
data.

Tested with SQLite3, but *may* work for other DB-API 2.0 databases with code
modifications. In particular, database data types are mapped between numpy data
types and SQLite datatypes, and will likely break for other databases. Other
examples of SQLite3-isms is querying the tables through `pragma table_info`.

---

MIT License

Copyright 2022 Jonathan Woodring

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import numpy
from csv import reader, writer
from collections import defaultdict
import gzip

def _csv_datatype(value):
    try:
        value = int(value)
        return 'int'
    except:
        try:
            value = float(value)
            return 'real'
        except:
            return 'string'

_np_datatype = defaultdict(lambda: 'string')
_np_datatype[numpy.dtype('float32').str] = 'real'
_np_datatype[numpy.dtype('float64').str] = 'real'
_np_datatype[numpy.dtype('int32').str] = 'int'
_np_datatype[numpy.dtype('int64').str] = 'int'

_np_convert = defaultdict(lambda: str)
_np_convert[numpy.dtype('float32').str] = float
_np_convert[numpy.dtype('float64').str] = float
_np_convert[numpy.dtype('int32').str] = int
_np_convert[numpy.dtype('int64').str] = int

def _validate(conn, query, dtypes):
    cursor = conn.cursor()
    cursor.execute(query)
    row = cursor.fetchone()
    if row is None:
        raise ValueError('empty query result')
    if not (dtypes is None) and len(dtypes) != len(row):
        raise ValueError('the length of dtypes needs to be the same as the number of columns')
    return row, cursor, [i[0] for i in cursor.description]

def columnnames(conn, query):
    """
    Given a SQL DB-API 2.0, `conn`, return the names of the columns from a SQL
    `query` on `conn` as a list of strings.

    :param conn: A database connection object
    :type conn: SQL DB-API 2.0 object

    :param query: A SQL query on the database in `conn`
    :type query: str

    :return: A list of column names
    :rtype: List[str]
    """
    row, cursor, names = _validate(conn, query, None)

    return names

def columnnamestypes(conn, query):
    """
    Given a SQL DB-API 2.0, `conn`, return the names and numpy data types of
    the columns from a SQL `query` on `conn` as a list of 2-tuples with strings.

    :param conn: A database connection object
    :type conn: SQL DB-API 2.0 object

    :param query: A SQL query on the database in `conn`
    :type query: str

    :return: A list of tuples with column names and numpy data types
    :rtype: List[Tuple[str,str]]
    """
    row, cursor, names = _validate(conn, query, None)

    return [(n, t) for n, t in \
        zip(names, [numpy.array([i]).dtype.str for i in row])]

def tableschema(conn, table):
    """
    Given a SQL DB-API 2.0, `conn`, return the names and database data types of
    the columns for a `table` in `conn` as a list of 2-tuples with strings.

    :param conn: A database connection object
    :type conn: SQL DB-API 2.0 object

    :param table: A SQL table on the database in `conn`
    :type table: str

    :return: A list of tuples with column names and SQLite data types
    :rtype: List[Tuple[str,str]]
    """
    return \
      [(i[1], i[2].lower()) for i in \
       conn.execute("pragma table_info(%s)" % table).fetchall()]

def tablenames(conn):
    """
    Given a SQL DB-API 2.0, `conn`, return the names of the tables in `conn` as
    a list of strings.

    *Note*: Likely only works with SQLite3 since it queries the `sqlite_schema`
    table for the list of tables.

    :param conn: A database connection object
    :type conn: SQL DB-API 2.0 object

    :return: A list of table names
    :rtype: List[str]
    """
    return [i[0] for i in conn.execute(
    "select name from sqlite_schema where type='table' and name not like 'sqlite_%'")]

def query2colarr(conn, query, dtypes=None):
    """
    Given a SQL DB-API 2.0, `conn`, return the data from a SQL `query` as
    a list of numpy arrays in column order.

    *colarr* refers to "column arrays," i.e., not row-oriented. `dtypes` is
    None to have numpy infer the data types for each column, or a list of numpy
    data types or None to specify the data type for each column (or infer the
    type with None).

    Indexing the data is [column] or [column][row], where column is the column
    index as an integer ordinal from and including 0. row is array integer
    index.

    :param conn: A database connection object
    :type conn: SQL DB-API 2.0 object

    :param query: A SQL query on the database in `conn`
    :type query: str

    :param dtypes: None or a list of numpy data types or None
    :type dtypes: Union[None,List[Union[numpy.dtype,None]]] = None

    :return: A list of numpy arrays representing the query as column data
    :rtype: List[numpy.array]
    """

    row, cursor, names = _validate(conn, query, dtypes)
    if dtypes is None:
        dtypes = [None]*len(row)

    columns = [[i] for i in row]
    for row in cursor:
        for i, c in zip(row, columns):
            c.append(i)
    return [numpy.array(c, d) for c, d in zip(columns, dtypes)]

def query2coldict(conn, query, dtypes=None):
    """
    Given a SQL DB-API 2.0, `conn`, return the data from a SQL `query` as
    a dictionary of column name to numpy array.

    *coldict* refers to "column dictionary," i.e., not row-oriented. `dtypes`
    is None to have numpy infer the data types for each column, or a list of
    numpy data types or None to specify the data type for each column (or infer
    the type with None).

    Indexing the data is [column] or [column][row], where column is the column
    index as the column name. row is array integer index.

    *Note:* if the query has duplicate column names, like `select x, x from
    a_table`, the dictionary will only have the right-most column data with
    that name. This can be fixed by using a renaming a column in the query with
    "as," like `select x, x as y from a_table`.

    :param conn: A database connection object
    :type conn: SQL DB-API 2.0 object

    :param query: A SQL query on the database in `conn`
    :type query: str

    :param dtypes: None or a list of numpy data types or None
    :type dtypes: Union[None,List[Union[numpy.dtype,None]]] = None

    :return: A dictionary of column name to numpy arrays representing the
             query as column data
    :rtype: Dict[str,numpy.array]
    """

    row, cursor, names = _validate(conn, query, dtypes)
    if dtypes is None:
        dtypes = [None]*len(row)

    columns = [[i] for i in row]
    for row in cursor:
        for i, c in zip(row, columns):
            c.append(i)
    return {n: numpy.array(c, d) for c, n, d in zip(columns, names, dtypes)}

def query2array(conn, query, dtype=None):
    """
    Given a SQL DB-API 2.0, `conn`, return the data from a SQL `query` as
    a 2D numpy array with a single data type.

    `dtype` is None to have numpy infer the data type for the entire array,
    or a numpy data type to specify the data type for the array.

    Indexing the data is [row], [row][column], [row, column], or [:,column],
    using numpy indexing rules. Indices are integers.

    :param conn: A database connection object
    :type conn: SQL DB-API 2.0 object

    :param query: A SQL query on the database in `conn`
    :type query: str

    :param dtype: None or a numpy data type
    :type dtypes: Union[numpy.dtype,None] = None

    :return: A 2D numpy array with a single data type
    :rtype: 2D numpy.array
    """

    row, cursor, names = _validate(conn, query, None)

    rows = [row]
    for row in cursor:
        rows.append(row)
    return numpy.array(rows, dtype)

def query2struct(conn, query, dtypes):
    """
    Given a SQL DB-API 2.0, `conn`, return the data from a SQL `query` as
    a 2D numpy structured array.

    `dtypes` is a list of numpy data types to specify the data type for each
    column. Structured arrays cannot infer data types and will try to cast
    everything to floating point, therefore we have to specify the types
    for each column.

    Indexing the data is [row], [row][column], or [column_name], using numpy
    indexing rules. Indices are integers, except for column_name indexing,
    which are strings.

    *Note:* if the query cannot have duplicate column names, like `select x, x
    from a_table`. This can be fixed by using a renaming a column in the query
    with "as," like `select x, x as y from a_table`.

    :param conn: A database connection object
    :type conn: SQL DB-API 2.0 object

    :param query: A SQL query on the database in `conn`
    :type query: str

    :param dtypes: A list of numpy data types
    :type dtypes: List[numpy.dtype]

    :return: A 1D structured numpy array with data types for each column
    :rtype: 1D structured numpy.array
    """

    row, cursor, names = _validate(conn, query, dtypes)
    if dtypes is None:
        dtypes = [None]*len(row)

    rows = [row]
    for row in cursor:
        rows.append(row)
    return numpy.array(rows, [(n, d) for n, d in zip(names, dtypes)])

def query2csv(conn, query, filename, header_skip=False, csv_options={},
    encoding='utf-8'):
    """
    Given a SQL DB-API 2.0, `conn`, write the data from a SQL `query` into
    a CSV file named `filename`.

    :param conn: A database connection object
    :type conn: SQL DB-API 2.0 object

    :param query: A SQL query on the database in `conn`
    :type query: str

    :param filename: The filename to write the CSV data to
    :type filename: str

    :param header_skip: If True, skip writing the header into the CSV
    :type header_skip: bool = False

    :param csv_options: Keyword arguments to pass to the CSV writer
    :type csv_options: Dict[str,Any] = {}

    :param encoding: Encoding of the CSV file
    :type encoding: str = 'utf-8'

    :return: None
    :rtype: None
    """

    row, cursor, names = _validate(conn, query, None)

    columns = [[i] for i in row]
    for row in cursor:
        for i, c in zip(row, columns):
            c.append(i)

    f = open(filename, 'w', encoding=encoding)
    w = writer(f, **csv_options)
    if not header_skip:
      w.writerow(names)
    for row in zip(*columns):
        w.writerow(row)
    f.close()

def csv2sqlite(conn, table, filename, header_skip=False,
    csv_options={}, encoding='utf-8', header=None, gzipped=False):
    """
    Given a SQL DB-API 2.0, `conn`, inject the data from a CSV `filename` into
    the database with the `table` name.

    :param conn: A database connection object
    :type conn: SQL DB-API 2.0 object

    :param table: A SQL table on the database in `conn`
    :type table: str

    :param filename: The filename to read the CSV data from
    :type filename: str

    :param header_skip: If True, skip reading a header from the CSV; if
      `header_skip` is True, `header` cannot be None
    :type header_skip: bool = False

    :param csv_options: Keyword arguments to pass to the CSV reader
    :type csv_options: Dict[str,Any] = {}

    :param encoding: Encoding of the CSV file
    :type encoding: str = 'utf-8'

    :param header: If None, uses the first line from the CSV to determine
      column names; else, a list of column names. Must be a list of column
      names if `header_skip` is True
    :type header: Union[None,List[str]]

    :return: A list of 2-tuples of column names and SQL data types
    :rtype: List[Tuple[str,str]]
    """

    if gzipped:
      f = gzip.open(filename, 'rt', encoding=encoding)
    else:
      f = open(filename, 'r', encoding=encoding)
    r = reader(f, **csv_options)

    if header_skip:
        if header is None:
            raise ValueError('header cannot be None when header_skip is True')
        columns = header
    else:
        if header is None:
            columns = next(r)
        else:
            next(r)
            columns = header
    row = next(r)
    types = [_csv_datatype(i) for i in row]

    cursor = conn.cursor()
    columnstr = ""
    insertstr = "insert into '%s' values (?" % table
    columnstr = "'%s' %s" % (columns[0], types[0])
    for n, t in zip(columns[1:], types[1:]):
        columnstr = columnstr + ", '%s' %s" % (n, t)
        insertstr = insertstr + ", ?"
    insertstr = insertstr + ")"
    cursor.execute("create table '%s' (%s)" % (table, columnstr))
    cursor.execute(insertstr, row)
    for row in r:
        cursor.execute(insertstr, row)
    conn.commit()
    f.close()

    return list(zip(columns, types))

def columns2sqlite(conn, table, columns, header):
    """
    Given a SQL DB-API 2.0, `conn`, inject the data from column arrays or
    dictionary arrays into the database with the `table` name.

    :param conn: A database connection object
    :type conn: SQL DB-API 2.0 object

    :param table: A SQL table on the database in `conn`
    :type table: str

    :param columns: A list of iterables (similar data returned by query2colarr),
      or a dictionary of column name to iterables (similar data returned by
      query2coldict)
    :type columns: Union[List[Iterable],Dict[str,Iterable]]

    :param header: If `columns` is a list of iterables, then `header` is
      a list of 2-tuples of column name and column index; else a list of
      column names (since `header` is a dictionary of column names to
      iterables)
    :type header: Union[List[Tuple[str,int],List[str]]]

    :return: A list of 2-tuples of column names and SQL data types
    :rtype: List[Tuple[str,str]]
    """

    if isinstance(header[0], tuple):
        names = [i[0] for i in header]
        idx = [i[1] for i in header]
    else:
        idx = header
        names = header
    columns = {i: numpy.array(columns[i]) for i in idx}
    types = [_np_datatype[columns[i].dtype.str] for i in idx]
    conv = [_np_convert[columns[i].dtype.str] for i in idx]

    cursor = conn.cursor()
    columnstr = ""
    insertstr = "insert into '%s' values (?" % table
    columnstr = "'%s' %s" % (names[0], types[0])
    for n, t in zip(names[1:], types[1:]):
        columnstr = columnstr + ", '%s' %s" % (n, t)
        insertstr = insertstr + ", ?"
    insertstr = insertstr + ")"
    cursor.execute("create table '%s' (%s)" % (table, columnstr))
    for row in zip(*[columns[i] for i in idx]):
        cursor.execute(insertstr, [c(v) for c, v in zip(conv, row)])
    conn.commit()

    return list(zip(names, types))
