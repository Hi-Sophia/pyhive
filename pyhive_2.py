from pyhive import hive
import sys
class HiveClient(object):
    def __init__(self,host,port,database):
        '''
        init the hiveClient
        :param host:
        :param port:
        :param database:
        '''
        self.host = host
        self.port = port
        self.database = database
    def getConnection(self):
        '''
        get the connection of the hive
        :return:
        '''
        print("获取hive客户端连接...........")
        return hive.Connection(host=self.host,port=self.port,database=self.database)
    def close(self,conn=None,cursor=None):
        '''
        close the hiveClient
        :param conn:
        :param cursor:
        :return:
        '''
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()
        print("关闭hive客户端连接...........")
class HiveOperator(object):
    def __init__(self,cursor,stdin):
        self.cursor = cursor
        self.stdin = stdin

    def execute(self):
        '''
        执行sql文件
        :return:
        '''
        # self.cursor.execute("source {HQL}".format(HQL=self.stdin[-1]))
        # self.cursor.execute('show tables')
        sql = open(self.stdin[-1]).read()
        print(sql.format(tables='tables'))
        self.cursor.execute(sql.format(tables='tables'))
        for result in self.cursor.fetchall():
            print(result)
def main():
    stdin = sys.argv
    if len(stdin) != 3:
        print("input format is error:", "please input <db> <sql file>")
        sys.exit(1)
    '''accept the input data'''
    db_name = stdin[1]

    ''' 获取hivethrift连接 '''
    try:
        hiveClient = HiveClient(host='localhost', port=10000, database=db_name)
        conn = hiveClient.getConnection()
        cursor = conn.cursor()
        '''执行sql文件'''
        operator = HiveOperator(cursor=cursor, stdin=stdin)
        operator.execute()
        '''关闭hive连接'''

        return 0
    except Exception as e:
        print(e)
        return 1
    finally:
        hiveClient.close(conn=conn, cursor=cursor)
if __name__=='__main__':

    main()
