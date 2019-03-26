import datetime
import os
import unittest
from copy import deepcopy
from unittest.mock import patch, call, ANY, MagicMock

import sqlalchemy
from xylose.scielodocument import Journal, Article

from documentstore_migracao.export import journal, article
from documentstore_migracao import exceptions, config
from . import SAMPLES_JOURNAL, SAMPLES_ARTICLE, SAMPLES_PATH, utils


@patch("documentstore_migracao.export.journal.request.get")
class TestExportJournal(unittest.TestCase):
    def test_ext_journal(self, mk_request_get):

        mk_request_get.return_value.json.return_value = [SAMPLES_JOURNAL]

        result = journal.ext_journal("1234-5678")
        mk_request_get.assert_called_once_with(
            ANY, params={"collection": ANY, "issn": "1234-5678"}
        )

        self.assertEqual(result.title, SAMPLES_JOURNAL["v100"][0]["_"])

    def test_ext_identifiers(self, mk_request_get):

        journal.ext_identifiers()
        mk_request_get.assert_called_once_with(ANY, params={"collection": ANY})

    @patch("documentstore_migracao.export.journal.ext_identifiers")
    @patch("documentstore_migracao.export.journal.ext_journal")
    def test_get_all_journal(self, mk_ext_journal, mk_ext_identifiers, mk_r):

        obj_journal = Journal(SAMPLES_JOURNAL)
        mk_ext_identifiers.return_value = {
            "objects": ["ANY", "ANY", {"code": "36341997000100001"}]
        }
        mk_ext_journal.return_value = obj_journal

        result = journal.get_all_journal()
        self.assertEqual(result[0], obj_journal)

    @patch("documentstore_migracao.export.article.RestfulClient.journals")
    def test_get_journals(self, mk_journals, mk_r):

        journal.get_journals()
        mk_journals.assert_called_once_with(collection=ANY)


class TestExportArticle(unittest.TestCase):
    @patch("documentstore_migracao.export.article.request.get")
    def test_ext_identifiers(self, mk_request_get):

        article.ext_identifiers("1234-5678")
        mk_request_get.assert_called_once_with(
            ANY, params={"collection": ANY, "issn": "1234-5678"}
        )

    @patch("documentstore_migracao.export.article.request.get")
    def test_ext_article(self, mk_request_get):

        result = article.ext_article("S0036-36341997000100001")
        mk_request_get.assert_called_once_with(
            ANY, params={"collection": ANY, "code": "S0036-36341997000100001"}
        )

    @patch("documentstore_migracao.export.article.ext_article")
    def test_ext_article_json(self, mk_ext_article):

        result = article.ext_article_json("S0036-36341997000100001")
        mk_ext_article.assert_called_once_with("S0036-36341997000100001")

    @patch("documentstore_migracao.export.article.ext_article")
    def test_ext_article_txt(self, mk_ext_article):

        result = article.ext_article_txt("S0036-36341997000100001")
        mk_ext_article.assert_called_once_with(
            "S0036-36341997000100001", body="true", format="xmlrsps"
        )

    @patch("documentstore_migracao.export.article.get_articles")
    def test_get_all_articles_notXML(self, mk_get_articles):

        mk_get_articles.return_value = [Article(SAMPLES_ARTICLE)]
        result = article.get_all_articles_notXML("0036-3634")
        self.assertEqual(result[0][0], "S0036-36341997000100001")

    @patch("documentstore_migracao.export.article.get_articles")
    def test_get_all_articles_notXML_not_xml(self, mk_get_articles):

        copy_SAMPLES_ARTICLE = deepcopy(SAMPLES_ARTICLE)
        copy_SAMPLES_ARTICLE["version"] = "xml"

        mk_get_articles.return_value = [Article(copy_SAMPLES_ARTICLE)]
        result = article.get_all_articles_notXML("0036-3634")
        self.assertEqual(result, [])

    @patch("documentstore_migracao.export.article.RestfulClient.documents")
    def test_ext_article(self, mk_documents):

        result = article.get_articles("1234-5678")
        mk_documents.assert_called_once_with(collection=ANY, issn="1234-5678")

    @patch("documentstore_migracao.export.article.ext_article_txt")
    def test_get_not_xml_article(self, mk_ext_article_txt):

        obj = Article(SAMPLES_ARTICLE)
        article.get_not_xml_article(obj)
        mk_ext_article_txt.assert_called_once_with("S0036-36341997000100001")

    @patch("documentstore_migracao.export.article.ext_article_txt")
    def test_get_not_xml_article_xml(self, mk_ext_article_txt):

        copy_SAMPLES_ARTICLE = deepcopy(SAMPLES_ARTICLE)
        copy_SAMPLES_ARTICLE["version"] = "xml"

        obj = Article(copy_SAMPLES_ARTICLE)
        article.get_not_xml_article(obj)
        mk_ext_article_txt.assert_not_called()


class TestJournalManagerDB(unittest.TestCase):
    def setUp(self):
        self.db_uri = config.get("SCIELO_MANAGER_DB")
        self.journal_manager_db = journal.JournalManagerDB(uri=self.db_uri)

    def test_init(self):
        self.assertEqual(self.journal_manager_db._uri, self.db_uri)
        self.assertIsNone(self.journal_manager_db._engine)
        self.assertIsNone(self.journal_manager_db._connection)
        self.assertIsNone(self.journal_manager_db._metadata)

    @patch("documentstore_migracao.export.journal.sqlalchemy.create_engine")
    def test_engine_new_engine_if_no_engine(self, mock_create_engine):
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        self.assertEqual(
            self.journal_manager_db.engine, self.journal_manager_db._engine)
        mock_create_engine.assert_called_once_with(self.db_uri, echo=True)
        self.assertEqual(self.journal_manager_db._engine, mock_engine)

    @patch("documentstore_migracao.export.journal.sqlalchemy.create_engine")
    def test_engine_returns_engine_if_engine_already_exists(
        self,
        mock_create_engine
    ):
        self.journal_manager_db._engine = "Engine"
        self.assertEqual(
            self.journal_manager_db.engine, self.journal_manager_db._engine)
        mock_create_engine.assert_not_called()

    @patch("documentstore_migracao.export.journal.sqlalchemy.MetaData")
    def test_metadata_new_metadata_if_no_metadata(self, MockMetaData):
        mock_engine = MagicMock()
        self.journal_manager_db._engine = mock_engine
        MockMetaData.return_value = "MetaData"
        self.assertEqual(
            self.journal_manager_db.metadata, self.journal_manager_db._metadata)
        MockMetaData.assert_called_once_with(bind=mock_engine)
        self.assertEqual(self.journal_manager_db._metadata, "MetaData")

    @patch("documentstore_migracao.export.journal.sqlalchemy.MetaData")
    def test_metadata_returns_metadata_if_metadata_already_exists(self, MockMetaData):
        self.journal_manager_db._metadata = "MetaData"
        self.assertEqual(
            self.journal_manager_db.metadata, self.journal_manager_db._metadata)
        MockMetaData.assert_not_called()

    def test_conn_new_connection_if_no_connection(self):
        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.connect.return_value = mock_conn
        self.journal_manager_db._engine = mock_engine
        self.assertEqual(
            self.journal_manager_db.conn, self.journal_manager_db._connection)
        mock_engine.connect.assert_called_once_with()
        self.assertEqual(self.journal_manager_db._connection, mock_conn)

    def test_conn_returns_connection_if_connection_already_exists(self):
        mock_engine = MagicMock()
        self.journal_manager_db._connection = "B"
        self.assertEqual(
            self.journal_manager_db.conn, self.journal_manager_db._connection)
        mock_engine.connect.assert_not_called()

    @patch("documentstore_migracao.export.journal.sqlalchemy.create_engine")
    def test_execute_sql(
        self,
        mock_create_engine
    ):
        sql_command = "select * from table where id=1"
        mock_execute_result = MagicMock()
        mock_conn = MagicMock()
        mock_conn.execute.return_value = mock_execute_result
        self.journal_manager_db._connection = mock_conn
        self.journal_manager_db.execute_sql(sql_command)
        mock_conn.execute.assert_called_once_with(sql_command)
        mock_execute_result.fetchall.assert_called_once_with()
    
    @patch("documentstore_migracao.export.journal.sqlalchemy.create_engine")
    def test_execute_sql_raises_exception_if_execute_error(
        self,
        mock_create_engine
    ):
        sql_command = "select * from table where id=1"
        mock_execute_result = MagicMock()
        mock_conn = MagicMock()
        mock_conn.execute = MagicMock(
            side_effect=sqlalchemy.exc.ProgrammingError(sql_command, "", ""),
            return_value=mock_execute_result
        )
        self.journal_manager_db._connection = mock_conn
        self.assertRaises(
            journal.JournalManagerDBError,
            self.journal_manager_db.execute_sql,
            sql_command,
        )
        mock_execute_result.fetchall.assert_not_called()

    @patch("documentstore_migracao.export.journal.sqlalchemy.create_engine")
    def test_execute_sql_returns_fetch_result(
        self,
        mock_create_engine
    ):
        sql_command = "select * from table where id=1"
        sql_result = [
            (1, "a",),
            (1, "b",),
            (1, "c",),
        ]
        mock_execute_result = MagicMock()
        mock_execute_result.fetchall.return_value = sql_result
        mock_conn = MagicMock()
        mock_conn.execute.return_value = mock_execute_result
        self.journal_manager_db._connection = mock_conn
        result = self.journal_manager_db.execute_sql(sql_command)
        self.assertEqual(result, sql_result)

    @patch("documentstore_migracao.export.journal.sqlalchemy.create_engine")
    def test_collect_tables_names_exception(
        self,
        mock_create_engine
    ):
        with patch.object(self.journal_manager_db, "execute_sql") as mock_execute_sql:
            mock_execute_sql.side_effect = journal.JournalManagerDBError()
            self.assertRaises(
                journal.JournalManagerDBError,
                self.journal_manager_db.collect_tables_names
            )

    @patch("documentstore_migracao.export.journal.sqlalchemy.create_engine")
    def test_collect_tables_names(
        self,
        mock_create_engine
    ):
        with patch.object(self.journal_manager_db, "execute_sql") as mock_execute_sql:
            tables = [
                ('auth_user',),
                ('django_content_type',),
                ('django_flatpage',),
            ]
            mock_execute_sql.return_value = tables
            result = self.journal_manager_db.collect_tables_names()
            mock_execute_sql.assert_called_once_with(
                "select table_name from information_schema.tables "
                "where table_schema='public' and table_name like journalmanager_%"
            )
            self.assertEqual(result, [table[0] for table in tables])

    @patch("documentstore_migracao.export.journal.sqlalchemy.Table")
    @patch("documentstore_migracao.export.journal.sqlalchemy.create_engine")
    def test_get_table_struct_exception(
        self,
        mock_create_engine,
        MockTable
    ):
        MockTable.side_effect = sqlalchemy.exc.OperationalError("table_name", "", "")
        self.assertRaises(
            journal.JournalManagerDBError,
            self.journal_manager_db.get_table_struct,
            "table_name"
        )

    @patch("documentstore_migracao.export.journal.sqlalchemy.Table")
    @patch("documentstore_migracao.export.journal.sqlalchemy.create_engine")
    def test_get_table_struct(
        self,
        mock_create_engine,
        MockTable
    ):
        self.journal_manager_db._engine = "Engine"
        self.journal_manager_db._metadata = "MetaData"
        MockTable.return_value = "AuthUserTable"
        table_struct = self.journal_manager_db.get_table_struct("auth_user")
        MockTable.assert_called_once_with(
            "auth_user",
            "MetaData",
            autoload=True,
            autoload_with="Engine"
        )
        self.assertEqual(table_struct, "AuthUserTable")

    def test_get_collection_gets_table_struct(self):
        with patch.object(self.journal_manager_db, "get_table_struct") \
                as mock_get_table_struct:
            mock_get_table_struct.return_value = MagicMock()
            self.journal_manager_db.get_collection('spa')
            mock_get_table_struct.assert_called_once_with('journalmanager_collection')

    def test_get_collection_exception(self):
        mock_collection_table = MagicMock()
        mock_collection_table.select.return_value.where.return_value.\
            execute.return_value.fetchall.side_effect = sqlalchemy.exc.OperationalError(
                "table_name", "", "")
        with patch.object(self.journal_manager_db, "get_table_struct") \
                as mock_get_table_struct:
            mock_get_table_struct.return_value = mock_collection_table
            self.assertRaises(
                journal.JournalManagerDBError,
                self.journal_manager_db.get_collection,
                'spa'
            )

    def test_get_collection_returns_serialized_journal(self):
        columns = ['column_1', 'column_2', 'column_3', ]
        lines = [
            ('value_1_1', datetime.datetime(2018, 1, 2), 'value_1_3',),
            ('value_2_1', datetime.datetime(2018, 2, 2), 'value_2_3',),
            ('value_3_1', datetime.datetime(2018, 3, 2), 'value_3_3',),
        ]
        expected = {
            column: value
            for column, value in zip(columns, lines[0])
        }
        mock_collection_table = MagicMock()
        mock_collection_table.c.keys.return_value = columns
        mock_collection_table.select.return_value.where.return_value.\
            execute.return_value.fetchall.return_value = lines
        with patch.object(self.journal_manager_db, "get_table_struct") \
                as mock_get_table_struct:
            mock_get_table_struct.return_value = mock_collection_table
            journal_dict = self.journal_manager_db.get_collection('spa')
            self.assertEqual(journal_dict, expected)

    def test_get_journals_by_collection_gets_tables_structs(self):
        with patch.object(self.journal_manager_db, "get_table_struct") \
                as mock_get_table_struct:
            self.journal_manager_db.get_journals_by_collection('spa')
            calls = [
                call('journalmanager_collection'),
                call('journalmanager_journal'),
                call('journalmanager_membership')
            ]
            mock_get_table_struct.assert_has_calls(calls, any_order=True)

    def test_get_journals_by_collection_exception(self):
        mock_collection_table = MagicMock()
        mock_journal_table = MagicMock()
        mock_journal_table.select.return_value.select_from.return_value.\
            where.return_value.execute.return_value.fetchall.side_effect = \
            sqlalchemy.exc.OperationalError("table_name", "", "")
        mock_membership_table = MagicMock()
        tables_structs = [
            mock_collection_table,
            mock_journal_table,
            mock_membership_table,
        ]
        with patch.object(self.journal_manager_db, "get_table_struct") \
                as mock_get_table_struct:
            mock_get_table_struct.side_effect = tables_structs
            self.assertRaises(
                journal.JournalManagerDBError,
                self.journal_manager_db.get_journals_by_collection,
                'spa'
            )

    def test_get_journals_by_collection_returns_serialized_journal(self):
        columns = ['column_1', 'column_2', 'column_3', ]
        lines = [
            ('value_1_1', datetime.datetime(2018, 1, 2), 'value_1_3',),
            ('value_2_1', datetime.datetime(2018, 2, 2), 'value_2_3',),
            ('value_3_1', datetime.datetime(2018, 3, 2), 'value_3_3',),
        ]
        expected = [
            {
                column: value
                for column, value in zip(columns, line)
            }
            for line in lines
        ]
        mock_collection_table = MagicMock()
        mock_journal_table = MagicMock()
        mock_journal_table.c.keys.return_value = columns
        mock_journal_table.select.return_value.select_from.return_value.\
            where.return_value.execute.return_value.fetchall.return_value = lines
        mock_membership_table = MagicMock()
        tables_structs = [
            mock_collection_table,
            mock_journal_table,
            mock_membership_table,
        ]
        with patch.object(self.journal_manager_db, "get_table_struct") \
                as mock_get_table_struct:
            mock_get_table_struct.side_effect = tables_structs
            journal_dict = self.journal_manager_db.get_journals_by_collection('spa')
            self.assertEqual(journal_dict, expected)


class TestExportJournalFromManager(unittest.TestCase):
    @patch("documentstore_migracao.export.journal.JournalManagerDB")
    def test_config_database(self, MockJournalManagerDB):
        journal.ext_journal_from_manager()
        MockJournalManagerDB.assert_called_once_with(config.get("SCIELO_MANAGER_DB"))

    @patch.object(journal.JournalManagerDB, "get_journals_by_collection")
    @patch.object(journal.JournalManagerDB, "collect_tables_names")
    def test_collect_tables_names(
        self,
        mock_collect_tables_names,
        mock_get_journals_by_collection
    ):
        journal.ext_journal_from_manager()
        mock_collect_tables_names.assert_called_once_with()

    @patch.object(journal.JournalManagerDB, "get_journals_by_collection")
    @patch.object(journal.JournalManagerDB, "collect_tables_names")
    def test_get_journals_by_collection(
        self,
        mock_collect_tables_names,
        mock_get_journals_by_collection
    ):
        journal.ext_journal_from_manager()
        mock_get_journals_by_collection.assert_called_once_with(
            config.get("SCIELO_COLLECTION")
        )

    @patch.object(journal.JournalManagerDB, "get_journals_by_collection")
    @patch.object(journal.JournalManagerDB, "collect_tables_names")
    def test_save_journals(
        self,
        mock_collect_tables_names,
        mock_get_journals_by_collection
    ):
        journal.ext_journal_from_manager()
        mock_get_journals_by_collection.return_value = [
            # {
            #     "scielo_issn": "print",
            #     "is_indexed_scie": False,
            #     "id": 44,
            #     "eletronic_issn": "1518-8787",
            #     "publisher_country": "BR",
            #     "previous_title_id": None,
            #     "subject_descriptors": "saude coletiva\\r\\nsaude publica\\r\\nmicrobiologia",
            #     "title_iso": "Rev. sa\\u00fade p\\u00fablica",
            #     "title": "Revista de Sa\\u00fade P\\u00fablica",
            #     "short_title": "Rev. Sa\\u00fade P\\u00fablica",
            #     "secs_code": "",
            #     "editor_address_state": "SP",
            #     "print_issn": "0034-8910",
            #     "copyrighter": "Faculdade de Sa\\u00fade P\\u00fablica da Universidade de S\\u00e3o Paulo",
            #     "init_vol": "1",
            #     "publisher_state": "SP",
            #     "ctrl_vocabulary": "decs",
            #     "is_indexed_ssci": False,
            #     "url_online_submission": "http://mc04.manuscriptcentral.com/rsp-scielo",
            #     "creator_id": 11,
            #     "acronym": "rsp",
            #     "created": datetime.datetime(1998, 4, 30, 3),
            #     "init_year": "1967",
            #     "url_journal": ""
            # },
        ]
