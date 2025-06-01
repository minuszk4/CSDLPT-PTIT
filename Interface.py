from psycopg2 import OperationalError
import psycopg2

def getopenconnection(user='postgres', password='1', dbname='csdlpt'):
    try:
        conn = psycopg2.connect(dbname=dbname, user=user, password=password, host='localhost')
        print("Kết nối thành công đến database:", dbname)
        return conn
    except OperationalError as e:
        if 'database "{}" does not exist'.format(dbname) in str(e):
            print(f"Database {dbname} chưa tồn tại, sẽ tạo mới.")
            create_db(dbname, user=user, password=password)
            return getopenconnection(user=user, password=password, dbname=dbname)
        else:
            print("Lỗi khi kết nối đến database:", e)
            return None

def create_db(dbname='postgres', user='postgres', password='1'):
    con = getopenconnection(user=user, password=password, dbname='postgres')
    if con is None:
        print("Không thể kết nối đến postgres để tạo database")
        return
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=%s;', (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s;' % dbname)
        print("Database %s created." % dbname)
    else:
        print("Database %s already exists." % dbname)
    cur.close()

def LoadRatings(ratingstable, ratingsfilepath,connection):
    cur = connection.cursor()
    cur.execute("DROP TABLE IF EXISTS " + ratingstable + ";")
    cur.execute("CREATE TABLE " + ratingstable + "(userid integer, extra1 char, movieid integer, extra2 char, rating float, extra3 char, timestamp bigint);")
    cur.execute("TRUNCATE TABLE " + ratingstable + ";")
    connection.commit()
    try:
        cur.copy_from(open(ratingsfilepath), ratingstable, sep=':')
        cur.execute("ALTER TABLE " + ratingstable + " DROP COLUMN extra1, DROP COLUMN extra2, DROP COLUMN extra3, DROP COLUMN timestamp;")
    except IOError as e:
        print("Lỗi khi đọc file:", e)
        cur.close()
        return
    cur.close()
    connection.commit()

# def LoadRatings(ratingsfilepath,connection):
#     cur = connection.cursor()
#     cur.execute("DROP TABLE IF EXISTS ratings;")
#     cur.execute("CREATE TABLE ratings (userid integer, extra1 char, movieid integer, extra2 char, rating float, extra3 char, timestamp bigint);")
#     cur.execute("TRUNCATE TABLE ratings;")
#     connection.commit()
#     try:
#         cur.copy_from(open(ratingsfilepath), ratingstable, sep=':')
#         cur.execute("ALTER TABLE ratings DROP COLUMN extra1, DROP COLUMN extra2, DROP COLUMN extra3, DROP COLUMN timestamp;")
#     except IOError as e:
#         print("Lỗi khi đọc file:", e)
#         cur.close()
#         return
#     cur.close()
#     connection.commit()

def Range_Partition(ratingstable, N,connection):

    if N <= 0:
        print("Số phân vùng phải lớn hơn 0")
        return
    init_metadata_table(connection)
  
    delta = 5.0 / N
    PREFIX = 'range_part'
    cur = connection.cursor()
    cur.execute("""
        SELECT tablename 
        FROM pg_tables 
        WHERE tablename LIKE %s;
    """, (PREFIX + '%',))
    old_tables = cur.fetchall()
    
    for (table_name,) in old_tables:
        cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
    
    for i in range(N):
        minRange = i * delta
        maxRange = minRange + delta
        table_name = PREFIX + str(i)
        
        cur.execute(f"""
            CREATE TABLE {table_name} (
                userid integer,
                movieid integer,
                rating float
            );
        """)
        
        if i == 0:
            sql = f"""
                INSERT INTO {table_name} (userid, movieid, rating)
                SELECT userid, movieid, rating FROM {ratingstable}
                WHERE rating >= %s AND rating <= %s;
            """
            params = (minRange, maxRange)
        else:
            sql = f"""
                INSERT INTO {table_name} (userid, movieid, rating)
                SELECT userid, movieid, rating FROM {ratingstable}
                WHERE rating > %s AND rating <= %s;
            """
            params = (minRange, maxRange)
        
        cur.execute(sql, params)
    cur.close()
    update_metadata(connection, 'range', N)
    connection.commit()

def RoundRobin_Partition(ratingstable, N ,connection):

    if N <= 0:
        print("Số phân vùng phải lớn hơn 0")
        return
    init_metadata_table(connection)

    cur = connection.cursor()
    
    PREFIX = 'rrobin_part'
    cur.execute("""
        SELECT tablename 
        FROM pg_tables 
        WHERE tablename LIKE %s;
    """, (PREFIX + '%',))
    old_tables = cur.fetchall()
    
    for (table_name,) in old_tables:
        cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
    for i in range(N):
        table_name = PREFIX + str(i)
        cur.execute("CREATE TABLE " + table_name + " (userid integer, movieid integer, rating float);")
        cur.execute(
            "INSERT INTO " + table_name + " (userid, movieid, rating) "
            "SELECT userid, movieid, rating FROM (SELECT userid, movieid, rating, ROW_NUMBER() OVER () as rnum FROM " + ratingstable + ") as temp "
            "WHERE mod(temp.rnum - 1, %s) = %s;", (N, i))
    cur.close()
    update_metadata(connection, 'rrobin', N)

    connection.commit()

def RoundRobin_Insert(ratingstable, userid, itemid, rating,connection):
    cur = connection.cursor()
    PREFIX = 'rrobin_part'
    cur.execute("INSERT INTO " + ratingstable + "(userid, movieid, rating) VALUES (%s, %s, %s);", (userid, itemid, rating))
    cur.execute("SELECT count(*) FROM " + ratingstable + ";")
    total_rows = cur.fetchone()[0]
    # N = count_partitions(connection, PREFIX)
    N = get_partition_count_from_metadata(connection, 'rrobin')

    if N == 0:
        print("Không có bảng phân vùng round-robin nào tồn tại")
        cur.close()
        return
    index = (total_rows - 1) % N
    table_name = PREFIX + str(index)
    cur.execute("INSERT INTO " + table_name + "(userid, movieid, rating) VALUES (%s, %s, %s);", (userid, itemid, rating))
    cur.close()
    connection.commit()


def Range_Insert(ratingstable, userid, itemid, rating,connection):
    cur = connection.cursor()
    PREFIX = 'range_part'
    # N = count_partitions(connection,PREFIX)
    N = get_partition_count_from_metadata(connection, 'range')
    if N == 0:
        print("Không có bảng phân vùng range nào tồn tại")
        cur.close()
        return
    delta = 5.0 / N
    index = int(rating / delta)
    if rating % delta == 0 and index != 0:
        index -= 1
    table_name = PREFIX + str(index)
    cur.execute("INSERT INTO "+ratingstable+" (userid, movieid, rating) VALUES (%s, %s, %s);", (userid, itemid, rating))
    cur.execute("INSERT INTO " + table_name + "(userid, movieid, rating) VALUES (%s, %s, %s);", (userid, itemid, rating))
    cur.close()
    connection.commit()

# def count_partitions(connection, prefix):
#     cur = connection.cursor()
#     cur.execute("SELECT count(*) FROM pg_stat_user_tables WHERE relname LIKE %s;", (prefix + '%',))
#     count = cur.fetchone()[0]
#     cur.close()
#     return count

def init_metadata_table(connection):
    with connection.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS partitionmetadata (
                partitiontype TEXT PRIMARY KEY,
                partitioncount INTEGER
            );
        """)
        connection.commit()

def update_metadata(connection, partition_type, count):
    with connection.cursor() as cur:
        cur.execute("""
            INSERT INTO partitionmetadata (partitiontype, partitioncount)
            VALUES (%s, %s)
            ON CONFLICT (partitiontype) DO UPDATE
            SET partitioncount = EXCLUDED.partitioncount;
        """, (partition_type, count))
        connection.commit()

def get_partition_count_from_metadata(connection, partition_type):
    with connection.cursor() as cur:
        cur.execute("SELECT partitioncount FROM partitionmetadata WHERE partitiontype = %s;", (partition_type,))
        result = cur.fetchone()
        return result[0] if result else 0

def main():
    create_db('csdlpt')
    con = getopenconnection()
    if con is None:
        print("Không thể kết nối đến database. Thoát chương trình.")
        return

    try:
        LoadRatings('ratings', 'data/ratings.dat',con)
        Range_Partition('ratings', 5,con)
        RoundRobin_Partition('ratings', 5,con)
        RoundRobin_Insert('ratings', 1, 1, 4.5,con)
        Range_Insert('ratings', 2, 2, 3.0,con)
    except Exception as e:
        print("Lỗi trong quá trình xử lý:", e)
    finally:
        con.close()
        print("Đã đóng kết nối đến database.")

if __name__ == '__main__':
    main()