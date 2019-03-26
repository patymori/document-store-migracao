""" module to export journal data """
import datetime
import json
import logging

import sqlalchemy
from xylose.scielodocument import Journal
from articlemeta.client import RestfulClient

from documentstore_migracao import config
from documentstore_migracao.utils import request, extract_isis

logger = logging.getLogger(__name__)


issues_tables = [
    {
        'table_key': 'editorialmanager_editorialboard',
        'tables_names': [
            {
                'table_key': 'editorialmanager_editorialmember',
                'tables_names': [
                    {
                        'table_key': 'editorialmanager_roletype',
                        'tables_names': [
                            {'table_key': 'editorialmanager_roletypetranslation'},
                        ],
                    },
                ],
            },
        ],
    },
    {'table_key': 'journalmanager_issuetitle'},
    {'table_key': 'journalmanager_regularpressrelease'},
]


journals_tables = [
    {
        'table_key': 'journalmanager_issue',
        'tables_names': issues_tables,
    },
    {
        'table_key': 'journalmanager_aheadpressrelease',
        'tables_names': [
            {
                'table_key': 'journalmanager_pressrelease',
                'tables_names': [
                    {'table_key': 'journalmanager_pressreleasearticle'},
                    {'table_key': 'journalmanager_pressreleasetranslation'},
                ],
            },
        ],
    },
    {
        'table_key': 'journalmanager_section',
        'tables_names': [
            {'table_key': 'journalmanager_sectiontitle'},
            {'table_key': 'journalmanager_issue_section'},
        ],
    },
    {
        'table_key': 'journalmanager_journal_abstract_keyword_languages',
        'tables_names': [
            {'table_key': 'journalmanager_language'}
        ],
    },
    {
        'table_key': 'journalmanager_journal_subject_categories',
        'tables_names': [
            {'table_key': 'journalmanager_subjectcategory'},
        ],
    },
    {
        'table_key': 'journalmanager_journal_study_areas',
        'tables_names': [
            {'table_key': 'journalmanager_studyareas'},
        ],
    },
    {'table_key': 'journalmanager_journal_languages'},
    {'table_key': 'journalmanager_journaltitle'},
    {'table_key': 'journalmanager_journalmission'},
    {'table_key': 'journalmanager_uselicense'},
]


collection_tables = [
    {
        'table_key': 'journalmanager_membership',
        'tables_names': [
            {
                'table_key': 'journalmanager_journal',
                'tables_names': journals_tables,
            },
        ],
    },
    {
        'table_key': 'journalmanager_sponsor_collections',
        'tables_names': [
            {
                'table_key': 'journalmanager_sponsor',
                'tables_names': [
                    {'table_key': 'journalmanager_institution'},
                    {'table_key': 'journalmanager_journal_sponsor'},
                ],
            },
        ],
    },
    {'table_key': 'journalmanager_journaltimeline'},
]


db_tables = {
    'table_key': 'journalmanager_collection',
    'tables_names': collection_tables,
},


class JournalManagerDBError(Exception):
    """Exceção na execução de SQL query"""


class JournalManagerDB:
    def __init__(self, uri):
        self._uri = uri
        self._engine = None
        self._connection = None
        self._metadata = None

    @property
    def engine(self):
        if not self._engine:
            self._engine = sqlalchemy.create_engine(self._uri, echo=True)
        return self._engine

    @property
    def metadata(self):
        if not self._metadata:
            self._metadata = sqlalchemy.MetaData(bind=self.engine)
        return self._metadata

    @property
    def conn(self):
        if not self._connection:
            self._connection = self.engine.connect()
        return self._connection

    def execute_sql(self, sql_command):
        try:
            result = self.conn.execute(sql_command)
        except sqlalchemy.exc.ProgrammingError as exc:
            raise JournalManagerDBError(str(exc)) from None
        else:
            return result.fetchall()

    def collect_tables_names(self):
        result = self.execute_sql(
            "select table_name from information_schema.tables "
            "where table_schema='public' and table_name like journalmanager_%"
        )
        return [table[0] for table in result]

    def get_table_struct(self, table_name):
        try:
            return sqlalchemy.Table(
                table_name, self.metadata, autoload=True, autoload_with=self.engine)
        except sqlalchemy.exc.OperationalError as exc:
            raise JournalManagerDBError(str(exc)) from None

    def get_collection(self, collection_acronym):
        collection = self.get_table_struct('journalmanager_collection')
        try:
            collections = collection.select().where(
                collection.c.acronym == collection_acronym
            ).execute().fetchall()
        except sqlalchemy.exc.OperationalError as exc:
            raise JournalManagerDBError(str(exc)) from None
        else:
            return {
                column: value
                for column, value in zip(collection.c.keys(), collections[0])
            }

    def get_journals_by_collection(self, collection_acronym):
        # def default(o):
        #     if isinstance(o, (datetime.date, datetime.datetime)):
        #         return o.isoformat()

        collection = self.get_table_struct('journalmanager_collection')
        journal = self.get_table_struct('journalmanager_journal')
        membership = self.get_table_struct('journalmanager_membership')
        try:
            journals = journal.select().select_from(
                journal.join(membership.join(collection))
            ).where(
                collection.c.acronym == collection_acronym
            ).execute().fetchall()
        except sqlalchemy.exc.OperationalError as exc:
            raise JournalManagerDBError(str(exc)) from None
        else:
            return [
                {
                    column: value
                    for column, value in zip(journal.c.keys(), line)
                }
                for line in journals
            ]


def ext_identifiers():

    journals_id = request.get(
        "%s/journal/identifiers/" % config.get("AM_URL_API"),
        params={"collection": config.get("SCIELO_COLLECTION")},
    ).json()
    return journals_id


def ext_journal(issn):

    journal = request.get(
        "%s/journal" % config.get("AM_URL_API"),
        params={"collection": config.get("SCIELO_COLLECTION"), "issn": issn},
    ).json()
    return Journal(journal[0])


def serialize_data(table_struct, relation_object):
    if relation_object:
        queryset = table_struct.select().select_from(
            table_struct.join(relation_object.join(collection))
        ).where(
            collection.c.acronym == collection_acronym
        ).execute().fetchall()
        [
            {
                column: value
                for column, value in zip(table_struct.c.keys(), line)
            }
            for line in queryset
        ]
    else:
        {
            column: value
            for column, value in zip(collection.c.keys(), collections[0])
        }


def read_and_save_tables(get_table_struct, db_tables, relation_object=None):
    table_struct = get_table_struct(db_tables['table_key'])
    serialized = serialize_data(table_struct, relation_object)
    save_data(serialized)

    for table in db_tables['tables_names'] or []:
        read_and_save_tables(get_table_struct, table, table_struct)


def ext_journal_from_manager():
    journal_manager_db = JournalManagerDB(config.get("SCIELO_MANAGER_DB"))
    tables = journal_manager_db.collect_tables_names()
    read_and_save_tables(get_table_struct, db_tables)


def get_all_journal():

    journals = []
    journals_id = ext_identifiers()
    for d_journal in journals_id["objects"][2:]:
        journals.append(ext_journal(d_journal["code"]))

    return journals


def get_journals():

    cl = RestfulClient()
    return cl.journals(collection=config.get("SCIELO_COLLECTION"))
