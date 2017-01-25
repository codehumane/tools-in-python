import pymssql
import mysql.connector
import psycopg2
from _mssql import login_timeout


class mssql:


    conn = '';
    cursor = '';


    def __init__(self, connection_option):
        self.conn = pymssql.connect(
            server=connection_option['host'],
            user=connection_option['user'],
            password=connection_option['password'],
            database=connection_option['dbname']
        );

        self.cursor = self.conn.cursor(as_dict=True);


    def selectOne(self, tblname, columns, condition={}):
        sql = "SELECT {} FROM {}".format((", ".join(columns)), tblname);
        sql += ' WHERE {};'.format(condition['statement']);
        self.cursor.execute(sql, tuple(condition['params']));
        row = self.cursor.fetchone();

        if row is None:
            return None;

        result = {};
        for idx in range(len(columns)):
            result[columns[idx]] = row[columns[idx]];

        return result;


    def selectAll(self, tblname, columns, condition={}):
        """ MSSQL 데이터를 조건에 일치하는 모든 row를 select한다.
        :param tblname: 테이블명
        :type tblname: str
        :param columns: 조회할 컬럼명
        :type columns: ary
        :param condition: 조회 조건
        :type condition: str
        :returns: 조회된 row들의 column과 값으로 구성된 Sets의 배열
        """

        sql = "SELECT {} FROM {}".format((", ".join(columns)), tblname);
        if condition != {}:
            sql += ' WHERE {};'.format(condition['statement']);
            self.cursor.execute(sql, tuple(condition['params']));
        else:
            sql += ";";
            self.cursor.execute(sql);

        rows = self.cursor.fetchall();
        results = [];
        for row in rows:
            result = {};
            for idx in range(len(columns)):
                result[columns[idx]] = row[columns[idx]];
            results.append(result);

        return results;


    def execute(self, sql):
        return self.cursor.execute(sql);



class pg:


    # pgsql connection & cursor
    conn = '';
    cursor = '';


    # PgSQL DB에 연결한다.
    def __init__(self, connection_option):
        """ PgSQL ORM을 인스턴스화한다.
        :param connection_option: PgSQL 커넥션 파라미터
        :type connection_option: Sets
        """

        self.conn = psycopg2.connect(
            host=connection_option['host'],
            dbname=connection_option['dbname'],
            user=connection_option['user'],
            password=connection_option['password']
        );

        self.cursor = self.conn.cursor();

    def commit(self):
        """ 현재 트랜잭션을 커밋한다."""
        self.conn.commit();

    def rollback(self):
        """ 현재 트랜잭션을 롤백한다."""
        self.conn.rollback();

    def insert(self, tblname, dataset, idreturn=True):
        """ PgSQL 데이터를 insert한다.
        :param tblname: 테이블명
        :type tblname: str
        :param dataset: 컬럼(key)과 입력 데이터(value)
        :type dataset: Sets
        :returns: 생성된 row의 아이디
        """

        columns = [];
        values = [];
        params = [];
        sql = "";

        for k, v in dataset.items():
            params.append('%s');
            columns.append(str(k));
            if (v is None):
                values.append(None);
            else:
                values.append(str(v));

        prepared_statement = "INSERT INTO {}({}) VALUES ({})".format( tblname, (", ".join(columns)), (", ".join(params)));
        if idreturn is True:
            prepared_statement += ' RETURNING id';
        prepared_statement += ';';

        self.cursor.execute(prepared_statement, tuple(values));
        self.conn.commit();

        if idreturn is True:
            return self.cursor.fetchone()[0];
        else:
            return 0;


    def update(self, tblname, dataset, condition={}):
        """ PgSQL 데이터를 update한다.
        :param tblname: 테이블명
        :type tblname: str
        :param dataset: 컬럼명(key)과 데이터(value)
        :type dataset: Sets
        :param condition: 조건문
        :type condition: set
        :returns: 생성된 row의 아이디
        """

        params = [];
        datasetList = [];
        for k, v in dataset.items():
            params.append(v);
            datasetList.append("{} = %s".format(k));

        sql = "UPDATE {} SET {}".format(tblname, ", ".join(datasetList));

        if condition != {}:
            sql += ' WHERE {};'.format(condition['statement']);
            params.extend(condition['params']);
        else:
            sql += ";";

        self.cursor.execute(sql, tuple(params));
        self.conn.commit();


    def selectOne(self, tblname, columns, condition={}):
        """ PgSQL 데이터를 한개의 row만 select한다.
        :param tblname: 테이블명
        :type tblname: str
        :param dataset: 조회할 컬럼명
        :type dataset: ary
        :param condition: 조회 조건
        :type condition: set
        :returns: 조회된 row들의 column과 값으로 구성된 Sets
        """

        sql = "SELECT {} FROM {}".format((", ".join(columns)), tblname);
        if condition != {}:
            if 'params' in condition:
                sql += ' WHERE {};'.format(condition['statement']);
                self.cursor.execute(sql, tuple(condition['params']));
            else:
                sql += ' {}'.format(condition['statement']);
                self.cursor.execute(sql);
        else:
            sql += ";";
            self.cursor.execute(sql);

        row = self.cursor.fetchone();
        if row == None:
            return None;

        result = {};
        for idx in range(len(columns)):
            result[columns[idx]] = row[idx];

        return result;


    def selectAll(self, tblname, columns, condition={}):
        """ PgSQL 데이터를 조건에 일치하는 모든 row를 select한다.
        :param tblname: 테이블명
        :type tblname: str
        :param columns: 조회할 컬럼명
        :type columns: ary
        :param condition: 조회 조건
        :type condition: str
        :returns: 조회된 row들의 column과 값으로 구성된 Sets의 배열
        """

        sql = "SELECT {} FROM {}".format((", ".join(columns)), tblname);
        if condition != {}:
            sql += ' WHERE {};'.format(condition['statement']);
            self.cursor.execute(sql, tuple(condition['params']));
        else:
            sql += ";";
            self.cursor.execute(sql);

        rows = self.cursor.fetchall();
        results = [];
        for row in rows:
            result = {};
            for idx in range(len(columns)):
                result[columns[idx]] = row[idx];
            results.append(result);

        return results;


    def count(self, tblname, condition={}):
        """ PgSQL 데이터를 조건에 일치하는 모든 row를 count한다.
        :param tblname: 테이블명
        :type tblname: str
        :param condition: 조회 조건
        :type dataset: str
        :returns: 갯수
        """

        sql = "SELECT count(*) FROM {}".format(tblname);
        if condition != {}:
            sql += ' WHERE {};'.format(condition['statement']);
            self.cursor.execute(sql, tuple(condition['params']));
        else:
            sql += ";";
            self.cursor.execute(sql);

        result = self.cursor.fetchone();
        return result[0];


    def execute(self, sql):
        self.cursor.execute(sql);
        return self.cursor.fetchall();

    def execute(self, sql, params):
        self.cursor.execute(sql, tuple(params));
        return self.cursor.fetchall();

