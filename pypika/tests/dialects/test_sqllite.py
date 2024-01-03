import unittest

from pypika import Table, Field, QueryException
from pypika.dialects import SQLLiteQuery


class SelectTests(unittest.TestCase):
    table_abc = Table("abc")

    def test_bool_true_as_one(self):
        q = SQLLiteQuery.from_("abc").select(True)

        self.assertEqual('SELECT 1 FROM "abc"', str(q))

    def test_bool_false_as_zero(self):
        q = SQLLiteQuery.from_("abc").select(False)

        self.assertEqual('SELECT 0 FROM "abc"', str(q))


class ReplaceTests(unittest.TestCase):
    def test_normal_replace(self):
        query = SQLLiteQuery.into("abc").replace("v1", "v2", "v3")
        expected_output = "REPLACE INTO \"abc\" VALUES ('v1','v2','v3')"
        self.assertEqual(expected_output, str(query))

    def test_replace_subquery(self):
        query = SQLLiteQuery.into("abc").replace(SQLLiteQuery.from_("def").select("f1", "f2"))
        expected_output = 'REPLACE INTO "abc" VALUES ((SELECT "f1","f2" FROM "def"))'
        self.assertEqual(expected_output, str(query))

    def test_insert_or_replace(self):
        query = SQLLiteQuery.into("abc").insert_or_replace("v1", "v2", "v3")
        expected_output = "INSERT OR REPLACE INTO \"abc\" VALUES ('v1','v2','v3')"
        self.assertEqual(expected_output, str(query))


class ReturningClauseTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.table_abc = Table('abc')

    def test_returning_from_missing_table_raises_queryexception(self):
        field_from_diff_table = Field('xyz', table=Table('other'))

        with self.assertRaisesRegex(QueryException, "You can't return from other tables"):
            (
                SQLLiteQuery.from_(self.table_abc)
                .where(self.table_abc.foo == self.table_abc.bar)
                .delete()
                .returning(field_from_diff_table)
            )

    def test_queryexception_if_returning_used_on_invalid_query(self):
        with self.assertRaisesRegex(QueryException, "Returning can't be used in this query"):
            SQLLiteQuery.from_(self.table_abc).select('abc').returning('abc')

    def test_no_queryexception_if_returning_used_on_valid_query_type(self):
        # No exceptions for insert, update and delete queries
        with self.subTest('DELETE'):
            SQLLiteQuery.from_(self.table_abc).where(self.table_abc.foo == self.table_abc.bar).delete().returning(
                "id"
            )
        with self.subTest('UPDATE'):
            SQLLiteQuery.update(self.table_abc).where(self.table_abc.foo == 0).set("foo", "bar").returning("id")
        with self.subTest('INSERT'):
            SQLLiteQuery.into(self.table_abc).insert('abc').returning('abc')

    def test_return_field_from_join_table(self):
        new_table = Table('xyz')
        q = (
            SQLLiteQuery.update(self.table_abc)
            .join(new_table)
            .on(new_table.id == self.table_abc.xyz)
            .where(self.table_abc.foo == 0)
            .set("foo", "bar")
            .returning(new_table.a)
        )

        self.assertEqual(
            'UPDATE "abc" '
            'JOIN "xyz" ON "xyz"."id"="abc"."xyz" '
            'SET "foo"=\'bar\' '
            'WHERE "abc"."foo"=0 '
            'RETURNING "xyz"."a"',
            str(q),
        )
