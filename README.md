# sqlitenumpy

Helper methods for converting to and from SQL row data to numpy array column
data.

Tested with SQLite3, but *may* work for other DB-API 2.0 databases with code
modifications. In particular, database data types are mapped between numpy data
types and SQLite datatypes, and will likely break for other databases. Other
examples of SQLite3-isms is querying the tables through `pragma table_info`.

# Installation options after cloning

1. Copy `sqlitenumpy.py` to wherever it's needed, it's self contained

2. Install user with pip: `pip install --user .`

3. Install system-wide with pip: `pip install .`

# Example

```python
# sqlite 3 comes with all python installations
from sqlite3 import connect

# create an non-volatile, on-disk database
# if it already exists, it will open it;
# otherwise, it will create a new empty database
db = connect('db.sqlite')

# some helper functions to insert and query
import sqlitenumpy as sn

# insert raw numpy arrays into a table
sn.columns2sqlite(db, 'mydataset',
                  {'x': numpy.array([0, 1, 2, 3]),
                   'y': numpy.array([3.0, 2.0, 1.0, 0.0]),
                   'z': numpy.array(['a', 'b', 'c', 'd'])},
                  ['x', 'y', 'z'])
# or csv data
sn.csv2sqlite(db, 'mtcars', 'data/mtcars.csv')
sn.csv2sqlite(db, 'iris', 'data/iris.csv')

# retrieve as a list of arrays (column oriented)
print(sn.query2colarr(db, 'select * from mydataset'))
# retrieve as a dict of arrays (column oriented)
print(sn.query2coldict(db, 'select * from mydataset'))
# retrieve as a structured array (row oriented)
print(sn.query2struct(db, 'select * from mydataset', [int, float, '<U1']))
# retrieve as a 2D array (row oriented)
print(sn.query2array(db, 'select * from mydataset'))
# retrieve as a CSV
sn.query2csv(db, 'select * from mydataset', 'new.csv')
```
