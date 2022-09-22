from unittest import TestCase

class UnitTests(TestCase):
  def setUp(self):
    pass

  def tearDown(self):
    pass

  def test_columnnames(self):
    from sqlite3 import connect
    import sqlitenumpy as sn

    db = connect(':memory:')

    sn.columns2sqlite(db, 'foo',
      {'x': [1, 2, 3], 'y': [3.0, 2.0, 1.0], 'z': ['a', 'b', 'c']},
      ['x', 'y', 'z'])
    self.assertEqual(sn.columnnames(db, 'select * from foo'), ['x', 'y', 'z'])
    sn.columns2sqlite(db, 'bar',
      {'x': [1, 2, 3], 'y': [3.0, 2.0, 1.0], 'z': ['a', 'b', 'c']},
      ['z', 'x'])
    self.assertEqual(sn.columnnames(db, 'select * from bar'), ['z', 'x'])

    self.assertEqual(sn.columnnames(db, 'select x from bar'), ['x'])
    self.assertEqual(sn.columnnames(db, 'select x, z from bar'), ['x', 'z'])
    self.assertEqual(sn.columnnames(db, 'select x, x from bar'), ['x', 'x'])

  def test_columnnamestypes(self):
    from sqlite3 import connect
    import sqlitenumpy as sn

    db = connect(':memory:')

    sn.columns2sqlite(db, 'foo',
      {'x': [1, 2, 3], 'y': [3.0, 2.0, 1.0], 'z': ['a', 'b', 'c']},
      ['x', 'y', 'z'])
    self.assertEqual(sn.columnnamestypes(db, 'select * from foo'),
        [('x', '<i8'), ('y', '<f8'), ('z', '<U1')])
    sn.columns2sqlite(db, 'bar',
      {'x': [1, 2, 3], 'y': [3.0, 2.0, 1.0], 'z': ['a', 'b', 'c']},
      ['z', 'x'])
    self.assertEqual(sn.columnnamestypes(db, 'select * from bar'),
      [('z', '<U1'), ('x', '<i8')])

    self.assertEqual(sn.columnnamestypes(db, 'select x from bar'),
        [('x', '<i8')])
    self.assertEqual(sn.columnnamestypes(db, 'select x, z from bar'),
        [('x', '<i8'), ('z', '<U1')])
    self.assertEqual(sn.columnnamestypes(db, 'select x, x from bar'),
        [('x', '<i8'), ('x', '<i8')])

  def test_tableschema(self):
    from sqlite3 import connect
    import sqlitenumpy as sn

    db = connect(':memory:')

    sn.columns2sqlite(db, 'foo',
      {'x': [1, 2, 3], 'y': [3.0, 2.0, 1.0], 'z': ['a', 'b', 'c']},
      ['x', 'y', 'z'])
    self.assertEqual(sn.tableschema(db, 'foo'),
        [('x', 'int'), ('y', 'real'), ('z', 'string')])

  def test_tablenames(self):
    from sqlite3 import connect
    import sqlitenumpy as sn

    db = connect(':memory:')

    sn.columns2sqlite(db, 'foo',
      {'x': [1, 2, 3], 'y': [3.0, 2.0, 1.0], 'z': ['a', 'b', 'c']},
      ['x', 'y', 'z'])
    self.assertEqual(sn.tablenames(db), ['foo'])

    sn.columns2sqlite(db, 'bar',
      {'x': [1, 2, 3], 'y': [3.0, 2.0, 1.0], 'z': ['a', 'b', 'c']},
      ['z', 'x'])
    self.assertEqual(sorted(sn.tablenames(db)), ['bar', 'foo'])

  def test_query2colarr(self):
    from sqlite3 import connect
    import sqlitenumpy as sn
    from numpy import array

    db = connect(':memory:')

    sn.columns2sqlite(db, 'foo',
      {'x': [1, 2, 3], 'y': [3.3, 2.2, 1.1], 'z': ['a', 'b', 'c']},
      ['x', 'y', 'z'])

    b = [array([1, 2, 3]), array([3.3, 2.2, 1.1]), array(['a', 'b', 'c'])]
    q = sn.query2colarr(db, 'select * from foo order by x')
    self.assertEqual(
      [(i == j).all() for i, j in zip(b, q)], [True, True, True])
    self.assertEqual([i.dtype for i in b], [i.dtype for i in q])

    b = [array([1, 2, 3]), array([3.3, 2.2, 1.1]), array(['a', 'b', 'c'])]
    b.reverse()
    q = sn.query2colarr(db, 'select z, y, x from foo order by z')
    self.assertEqual(
      [(i == j).all() for i, j in zip(b, q)], [True, True, True])
    self.assertEqual([i.dtype for i in b], [i.dtype for i in q])

    b = [array(['c', 'b', 'a'])]*3
    q = sn.query2colarr(db, 'select z, z, z from foo order by z desc')
    self.assertEqual(
      [(i == j).all() for i, j in zip(b, q)], [True, True, True])
    self.assertEqual([i.dtype for i in b], [i.dtype for i in q])

    b = [array(['a', 'b', 'c'])]*3
    t = [None, '<U2', '<U3']
    q = sn.query2colarr(db, 'select z, z, z from foo order by z', t)
    self.assertEqual(
      [(i == j).all() for i, j in zip(b, q)], [True, True, True])
    self.assertEqual(['<U1', '<U2', '<U3'], [i.dtype for i in q])

    b = [array([3]), array(['c'])]
    b.reverse()
    q = sn.query2colarr(db, 'select z, x from foo where x > 2')
    self.assertEqual(
      [(i == j).all() for i, j in zip(b, q)], [True, True])
    self.assertEqual([i.dtype for i in b], [i.dtype for i in q])

  def test_query2coldict(self):
    from sqlite3 import connect
    import sqlitenumpy as sn
    from numpy import array

    db = connect(':memory:')

    sn.columns2sqlite(db, 'foo',
      {'x': [1, 2, 3], 'y': [3.3, 2.2, 1.1], 'z': ['a', 'b', 'c']},
      ['x', 'y', 'z'])

    b = {'x': array([1, 2, 3]),
         'y': array([3.3, 2.2, 1.1]),
         'z': array(['a', 'b', 'c'])}
    q = sn.query2coldict(db, 'select * from foo order by x')
    self.assertEqual(
      [(i == j).all() for i, j in zip(b.values(), q.values())],
      [True, True, True])
    self.assertEqual(sorted(b.keys()), sorted(q.keys()))
    self.assertEqual(
        [b[k].dtype for k in sorted(b.keys())],
        [q[k].dtype for k in sorted(q.keys())])

    b = {'x': array([1, 2, 3]),
         'y': array([3.3, 2.2, 1.1]),
         'z': array(['a', 'b', 'c'])}
    q = sn.query2coldict(db, 'select z, y, x from foo order by z')
    self.assertEqual(
      [(b[k] == q[k]).all() for k in b.keys()],
      [True, True, True])
    self.assertEqual(sorted(b.keys()), sorted(q.keys()))
    self.assertEqual(
        [b[k].dtype for k in sorted(b.keys())],
        [q[k].dtype for k in sorted(q.keys())])

    b = {'z': array(['c', 'b', 'a'])}
    q = sn.query2coldict(db, 'select z, z, z from foo order by z desc')
    self.assertEqual(
      [(b[k] == q[k]).all() for k in b.keys()],
      [True])
    self.assertEqual(sorted(b.keys()), sorted(q.keys()))
    self.assertEqual(
        [b[k].dtype for k in sorted(b.keys())],
        [q[k].dtype for k in sorted(q.keys())])

    b = {'z': array(['a', 'b', 'c']),
         'x': array(['a', 'b', 'c']),
         'y': array(['a', 'b', 'c'])}
    q = sn.query2coldict(db, 'select z, z as x, z as y from foo order by z')
    self.assertEqual(
      [(b[k] == q[k]).all() for k in b.keys()],
      [True, True, True])
    self.assertEqual(sorted(b.keys()), sorted(q.keys()))
    self.assertEqual(
        [b[k].dtype for k in sorted(b.keys())],
        [q[k].dtype for k in sorted(q.keys())])

    b = {'z': array(['a', 'b', 'c'])}
    t = [None, '<U2', '<U3']
    q = sn.query2coldict(db, 'select z, z, z from foo order by z', t)
    self.assertEqual(
      [(b[k] == q[k]).all() for k in b.keys()],
      [True])
    self.assertEqual(sorted(b.keys()), sorted(q.keys()))
    self.assertEqual(
        ['<U3'],
        [q[k].dtype for k in sorted(q.keys())])

    b = {'x': array([3]), 'z': array(['c'])}
    q = sn.query2coldict(db, 'select z, x from foo where x > 2')
    self.assertEqual(
      [(b[k] == q[k]).all() for k in b.keys()],
      [True, True])
    self.assertEqual(sorted(b.keys()), sorted(q.keys()))
    self.assertEqual(
        [b[k].dtype for k in sorted(b.keys())],
        [q[k].dtype for k in sorted(q.keys())])

  def test_query2array(self):
    from sqlite3 import connect
    import sqlitenumpy as sn
    from numpy import array

    db = connect(':memory:')

    sn.columns2sqlite(db, 'foo',
      {'x': [1, 2, 3], 'y': [3.3, 2.2, 1.1], 'z': ['a', 'b', 'c']},
      ['x', 'y', 'z'])

    b = array([[1, 2, 3], [3.3, 2.2, 1.1], ['a', 'b', 'c']]).T
    q = sn.query2array(db, 'select * from foo order by x')
    self.assertTrue((b == q).all())
    self.assertEqual(b.dtype, q.dtype)

    b = array([[1, 2, 3], [3.3, 2.2, 1.1]]).T
    q = sn.query2array(db, 'select x, y from foo order by x')
    self.assertTrue((b == q).all())
    self.assertEqual(b.dtype, q.dtype)
    self.assertEqual('<f8', q.dtype)

    b = array([[1, 2, 3], [3.3, 2.2, 1.1]]).T
    b = b.astype('float32')
    q = sn.query2array(db, 'select x, y from foo order by x', 'float32')
    self.assertTrue((b == q).all())
    self.assertEqual('<f4', q.dtype)

    b = array([[1, 2, 3]]).T
    b = b.astype('int32')
    q = sn.query2array(db, 'select x from foo order by x', 'int32')
    self.assertTrue((b == q).all())
    self.assertEqual('<i4', q.dtype)

    b = array([[1, 2, 3]]).T
    b = b.astype('float64')
    q = sn.query2array(db, 'select x from foo order by x', 'float64')
    self.assertTrue((b == q).all())
    self.assertEqual('<f8', q.dtype)

    b = [[1, 2, 3], [3.3, 2.2, 1.1], ['a', 'b', 'c']]
    b.reverse()
    b = array(b).T
    q = sn.query2array(db, 'select z, y, x from foo order by z')
    self.assertTrue((b == q).all())
    self.assertEqual(b.dtype, q.dtype)

    b = array([['c', 'b', 'a']]*3).T
    q = sn.query2array(db, 'select z, z, z from foo order by z desc')
    self.assertTrue((b == q).all())
    self.assertEqual(b.dtype, q.dtype)

    b = array([['c'], [3]]).T
    q = sn.query2array(db, 'select z, x from foo where x > 2')
    self.assertTrue((b == q).all())
    self.assertEqual(b.dtype, q.dtype)

  def test_query2array(self):
    from sqlite3 import connect
    import sqlitenumpy as sn
    from numpy import array

    db = connect(':memory:')

    sn.columns2sqlite(db, 'foo',
      {'x': [1, 2, 3], 'y': [3.3, 2.2, 1.1], 'z': ['a', 'b', 'c']},
      ['x', 'y', 'z'])

    b = [[1, 2, 3], [3.3, 2.2, 1.1], ['a', 'b', 'c']]
    b = [(x, y, z) for x, y, z in zip(*b)]
    t = [('x', '<i4'), ('y', '<f4'), ('z', '<U1')]
    b = array(b, t)
    q = sn.query2struct(db, 'select * from foo order by x',
        ['<i4', '<f4', '<U1'])
    self.assertTrue((b == q).all())
    for c in ['x', 'y', 'z']:
      self.assertEqual(b[c].dtype, q[c].dtype)

    b = [[1, 2, 3], [3.3, 2.2, 1.1], ['a', 'b', 'c']]
    b.reverse()
    b = [(x, y, z) for x, y, z in zip(*b)]
    t = [('x', '<i4'), ('y', '<f4'), ('z', '<U1')]
    t.reverse()
    b = array(b, t)
    q = sn.query2struct(db, 'select z, y, x from foo order by z',
        ['<U1', '<f4', '<i4'])
    self.assertTrue((b == q).all())
    for c in ['x', 'y', 'z']:
      self.assertEqual(b[c].dtype, q[c].dtype)

    b = [array(['c', 'b', 'a'])]*3
    b = [(x, y, z) for x, y, z in zip(*b)]
    t = [('z', '<U1'), ('x', '<U1'), ('y', '<U1')]
    b = array(b, t)
    q = sn.query2struct(db, 'select z, z as x, z as y from foo order by z desc',
        ['<U1', '<U1', '<U1'])
    self.assertTrue((b == q).all())
    for c in ['x', 'y', 'z']:
      self.assertEqual(b[c].dtype, q[c].dtype)

    b = [array(['c', 'b', 'a'])]*3
    b = [(x, y, z) for x, y, z in zip(*b)]
    t = [('z', '<U1'), ('x', '<U2'), ('y', '<U3')]
    b = array(b, t)
    q = sn.query2struct(db, 'select z, z as x, z as y from foo order by z desc',
        ['<U1', '<U2', '<U3'])
    self.assertTrue((b == q).all())
    for c in ['x', 'y', 'z']:
      self.assertEqual(b[c].dtype, q[c].dtype)

    b = [['c'], [3]]
    b = [(x, y) for x, y in zip(*b)]
    t = [('z', '<U1'), ('x', '<i4')]
    b = array(b, t)
    q = sn.query2struct(db, 'select z, x from foo where x > 2',
        ['<U1', '<i4'])
    self.assertTrue((b == q).all())
    for c in ['x', 'z']:
      self.assertEqual(b[c].dtype, q[c].dtype)

  def test_query2csv(self):
    from sqlite3 import connect
    import sqlitenumpy as sn
    from numpy import array
    from filecmp import cmp

    db = connect(':memory:')

    sn.columns2sqlite(db, 'foo',
      {'x': [1, 2, 3], 'y y': [3.3, 2.2, 1.1], 'z': ['a a', 'b b', 'c c']},
      ['x', 'y y', 'z'])

    sn.query2csv(db, 'select * from foo order by x', 'one.csv')
    self.assertTrue(cmp('one.csv', 'regress/one.csv'))
    sn.query2csv(db, 'select * from foo order by x', 'onenoh.csv',
        header_skip=True)
    self.assertTrue(cmp('onenoh.csv', 'regress/onenoh.csv'))

    sn.query2csv(db, "select z, foo.'y y', x from foo order by z", 'two.csv')
    self.assertTrue(cmp('two.csv', 'regress/two.csv'))
    sn.query2csv(db, "select z, foo.'y y', x from foo order by z", 'twonoh.csv',
        header_skip=True)
    self.assertTrue(cmp('twonoh.csv', 'regress/twonoh.csv'))

    sn.query2csv(db, 'select z, z, z from foo order by z desc', 'three.csv')
    self.assertTrue(cmp('three.csv', 'regress/three.csv'))
    sn.query2csv(db, 'select z, z, z from foo order by z desc', 'threenoh.csv',
        header_skip=True)
    self.assertTrue(cmp('threenoh.csv', 'regress/threenoh.csv'))

    sn.query2csv(db, 'select z, x from foo where x > 2 order by z desc',
        'four.csv')
    self.assertTrue(cmp('four.csv', 'regress/four.csv'))
    sn.query2csv(db, 'select z, x from foo where x > 2 order by z desc',
        'fournoh.csv', header_skip=True)
    self.assertTrue(cmp('fournoh.csv', 'regress/fournoh.csv'))

  def test_csv2sqlite(self):
    from sqlite3 import connect
    import sqlitenumpy as sn
    from numpy import array

    db = connect(':memory:')

    r = sn.csv2sqlite(db, 'a', 'regress/one.csv')
    self.assertEqual(r, [('x', 'int'), ('y y', 'real'), ('z', 'string')])
    b = [[1, 2, 3], [3.3, 2.2, 1.1], ['a a', 'b b', 'c c']]
    qt = 'select * from a order by x'
    q = sn.query2colarr(db, qt)
    self.assertEqual(sn.columnnames(db, qt), ['x', 'y y', 'z'])
    self.assertEqual(
      [(i == j).all() for i, j in zip(b, q)], [True, True, True])

    r = sn.csv2sqlite(db, 'b', 'regress/onenoh.csv', header_skip=True,
        header=['a', 'b', 'c'])
    self.assertEqual(r, [('a', 'int'), ('b', 'real'), ('c', 'string')])
    b = [[1, 2, 3], [3.3, 2.2, 1.1], ['a a', 'b b', 'c c']]
    qt = 'select * from b order by a'
    q = sn.query2colarr(db, qt)
    self.assertEqual(sn.columnnames(db, qt), ['a', 'b', 'c'])
    self.assertEqual(
      [(i == j).all() for i, j in zip(b, q)], [True, True, True])

    r = sn.csv2sqlite(db, 'c', 'regress/two.csv')
    self.assertEqual(r, [('z', 'string'), ('y y', 'real'), ('x', 'int')])
    b = [[1, 2, 3], [3.3, 2.2, 1.1], ['a a', 'b b', 'c c']]
    b.reverse()
    qt = "select z, c.'y y', x from c order by z"
    q = sn.query2colarr(db, qt)
    self.assertEqual(sn.columnnames(db, qt), ['z', 'y y', 'x'])
    self.assertEqual(
      [(i == j).all() for i, j in zip(b, q)], [True, True, True])

    r = sn.csv2sqlite(db, 'd', 'regress/twonoh.csv', header_skip=True,
        header=['a', 'b', 'c'])
    self.assertEqual(r, [('a', 'string'), ('b', 'real'), ('c', 'int')])
    b = [[1, 2, 3], [3.3, 2.2, 1.1], ['a a', 'b b', 'c c']]
    b.reverse()
    qt = "select * from d order by a"
    q = sn.query2colarr(db, qt)
    self.assertEqual(sn.columnnames(db, qt), ['a', 'b', 'c'])
    self.assertEqual(
      [(i == j).all() for i, j in zip(b, q)], [True, True, True])

    r = sn.csv2sqlite(db, 'e', 'regress/three.csv', header=['i', 'j', 'k'])
    self.assertEqual(r, [('i', 'string'), ('j', 'string'), ('k', 'string')])
    b = [['c c', 'b b', 'a a']]*3
    qt = "select * from e order by i desc"
    q = sn.query2colarr(db, qt)
    self.assertEqual(sn.columnnames(db, qt), ['i', 'j', 'k'])
    self.assertEqual(
      [(i == j).all() for i, j in zip(b, q)], [True, True, True])

    r = sn.csv2sqlite(db, 'f', 'regress/threenoh.csv', header_skip=True,
        header=['a', 'b', 'c'])
    self.assertEqual(r, [('a', 'string'), ('b', 'string'), ('c', 'string')])
    b = [['c c', 'b b', 'a a']]*3
    qt = "select * from f order by a desc"
    q = sn.query2colarr(db, qt)
    self.assertEqual(sn.columnnames(db, qt), ['a', 'b', 'c'])
    self.assertEqual(
      [(i == j).all() for i, j in zip(b, q)], [True, True, True])

    r = sn.csv2sqlite(db, 'g', 'regress/four.csv')
    self.assertEqual(r, [('z', 'string'), ('x', 'int')])
    b = [['c c'], [3]]
    qt = "select * from g order by z"
    q = sn.query2colarr(db, qt)
    self.assertEqual(sn.columnnames(db, qt), ['z', 'x'])
    self.assertEqual(
      [(i == j).all() for i, j in zip(b, q)], [True, True])

    r = sn.csv2sqlite(db, 'h', 'regress/fournoh.csv', header_skip=True,
        header=['a', 'b'])
    self.assertEqual(r, [('a', 'string'), ('b', 'int')])
    b = [['c c'], [3]]
    qt = "select * from h order by a"
    q = sn.query2colarr(db, qt)
    self.assertEqual(sn.columnnames(db, qt), ['a', 'b'])
    self.assertEqual(
      [(i == j).all() for i, j in zip(b, q)], [True, True])

    r = sn.csv2sqlite(db, 'i', 'regress/one.csv', header=['a', 'b', 'c'])
    self.assertEqual(r, [('a', 'int'), ('b', 'real'), ('c', 'string')])
    self.assertEqual(sn.columnnames(db, 'select * from i'),
        ['a', 'b', 'c'])
    b = [[1, 2, 3], [3.3, 2.2, 1.1], ['a a', 'b b', 'c c']]
    qt = 'select * from i order by a'
    q = sn.query2colarr(db, qt)
    self.assertEqual(sn.columnnames(db, qt), ['a', 'b', 'c'])
    self.assertEqual(
      [(i == j).all() for i, j in zip(b, q)], [True, True, True])

  def test_columns2sqlite(self):
    from sqlite3 import connect
    import sqlitenumpy as sn

    db = connect(':memory:')

    r = sn.columns2sqlite(db, 'a',
      {'x': [1, 2, 3], 'y': [3.3, 2.2, 1.1], 'z': ['a a', 'b b', 'c c']},
      ['x', 'y', 'z'])
    self.assertEqual(r, [('x', 'int'), ('y', 'real'), ('z', 'string')])
    b = [[1, 2, 3], [3.3, 2.2, 1.1], ['a a', 'b b', 'c c']]
    qt = 'select * from a order by x'
    q = sn.query2colarr(db, qt)
    self.assertEqual(sn.columnnames(db, qt), ['x', 'y', 'z'])
    self.assertEqual(
      [(i == j).all() for i, j in zip(b, q)], [True, True, True])

    r = sn.columns2sqlite(db, 'b',
      [[1, 2, 3], [3.3, 2.2, 1.1], ['a a', 'b b', 'c c']],
      [('x', 0), ('y', 1), ('z', 2)])
    self.assertEqual(r, [('x', 'int'), ('y', 'real'), ('z', 'string')])
    b = [[1, 2, 3], [3.3, 2.2, 1.1], ['a a', 'b b', 'c c']]
    qt = 'select * from b order by x'
    q = sn.query2colarr(db, qt)
    self.assertEqual(sn.columnnames(db, qt), ['x', 'y', 'z'])
    self.assertEqual(
      [(i == j).all() for i, j in zip(b, q)], [True, True, True])

    r = sn.columns2sqlite(db, 'c',
      {'x': [1, 2, 3], 'y': [3.3, 2.2, 1.1], 'z': ['a a', 'b b', 'c c']},
      ['z', 'x'])
    self.assertEqual(r, [('z', 'string'), ('x', 'int')])
    b = [[1, 2, 3], ['a a', 'b b', 'c c']]
    b.reverse()
    qt = 'select * from c order by x'
    q = sn.query2colarr(db, qt)
    self.assertEqual(sn.columnnames(db, qt), ['z', 'x'])
    self.assertEqual(
      [(i == j).all() for i, j in zip(b, q)], [True, True])

    r = sn.columns2sqlite(db, 'd',
      [[1, 2, 3], [3.3, 2.2, 1.1], ['a a', 'b b', 'c c']],
      [('a', 1), ('b', 0)])
    self.assertEqual(r, [('a', 'real'), ('b', 'int')])
    b = [[1, 2, 3], [3.3, 2.2, 1.1]]
    b.reverse()
    qt = 'select * from d order by b'
    q = sn.query2colarr(db, qt)
    self.assertEqual(sn.columnnames(db, qt), ['a', 'b'])
    self.assertEqual(
      [(i == j).all() for i, j in zip(b, q)], [True, True])

