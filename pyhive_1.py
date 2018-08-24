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
    fields = []                 #store fromtable fields
    partitions_name = []        #store fromtable partition name
    partitions_value = []       #store fromtable partition value
    col = ''
    def __init__(self,cursor,stdin):
        self.cursor = cursor
        self.stdin = stdin
    def get_partition_information(self):
        '''
        :param stdin:
        :return:
        '''
        print("获取源表分区信息.............")
        self.cursor.execute("show partitions {fromtable}".format(fromtable=self.stdin[1]))
        for partition in self.cursor.fetchall():
            self.partitions_name.append(partition[0].split('=')[0])
            self.partitions_value.append(partition[0].split('=')[1])
        return self.partitions_name,self.partitions_value
    def get_fields(self):
        '''
        get the fields of fromtable
        :return:
        '''
        print("获取源表字段信息............")
        self.cursor.execute("desc {fromtable}".format(fromtable=self.stdin[1]))
        for field in self.cursor.fetchall():
            if field[0] is '':
                break
            self.fields.append(field[0])
        for x in self.partitions_name:
            if x in self.fields:
                self.fields.remove(x)
        self.col = ','.join(self.fields)
        return self.col
    def copy_table(self,flag):
        print("表格开始转换............")
        try:
            self.cursor.execute(
                'create table if not exists {totable} like {fromtable} stored as orc'.format(totable=self.stdin[2],
                                                                     fromtable=self.stdin[1]))
        except Exception as e:
            print(e)
        if flag == 1:
            for partition_value in self.partitions_value:
                self.cursor.execute(
                    'insert into table {totable} partition({partion}="{value}")select {col} from {fromtable} where {partion}="{value}"'. \
                        format(totable=self.stdin[2], partion=self.partitions_name[0], value=partition_value, col=self.col,
                               fromtable=self.stdin[1]))
            print("表格转换成功............")
        if flag == 0:
            self.cursor.execute(
                'insert into table {totable} partition({partion}="{value}")select {col} from {fromtable} where {partion}="{value}"'. \
                    format(totable=self.stdin[2], partion=self.partitions_name[0], value=self.stdin[-1], col=self.col, fromtable=self.stdin[1]))
            print("表格转换成功............")

def main():
    flag = 0
    stdin = sys.argv
    if len(stdin) != 4:
        print("input format is error:", "please input <fromdb.table><todb.table><data>")
        sys.exit(1)
    '''accept the input data'''
    host = stdin[1]
    from_db_name, from_table_name = stdin[1].split('.')
    if (stdin[-1].lower() == 'all'):
        flag = 1
    print("flag is:", flag)
    ''' 获取hivethrift连接 '''
    try:
        hiveClient = HiveClient(host='localhost', port=10000, database=from_db_name)
        conn = hiveClient.getConnection()
        cursor = conn.cursor()
        '''对表进行创建，转变存储格式 '''
        operator = HiveOperator(cursor=cursor, stdin=stdin)
        operator.get_partition_information()
        operator.get_fields()
        operator.copy_table(flag=flag)
        '''关闭hive连接'''
        hiveClient.close(conn=conn, cursor=cursor)
        return 0
    except Exception as e:
        print(e)
        return 1
if __name__=='__main__':

    main()
