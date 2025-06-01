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

def loadratings(tablename, ratingsfilepath,connection):
    cur = connection.cursor()
    cur.execute("DROP TABLE IF EXISTS " + tablename + ";")
    cur.execute("CREATE TABLE " + tablename + "(userid integer, extra1 char, movieid integer, extra2 char, rating float, extra3 char, timestamp bigint);")
    cur.execute("TRUNCATE TABLE " + tablename + ";")
    connection.commit()
    try:
        cur.copy_from(open(ratingsfilepath), tablename, sep=':')
        cur.execute("ALTER TABLE " + tablename + " DROP COLUMN extra1, DROP COLUMN extra2, DROP COLUMN extra3, DROP COLUMN timestamp;")
    except IOError as e:
        print("Lỗi khi đọc file:", e)
        cur.close()
        return
    cur.close()
    connection.commit()

def rangepartition(tablename, N,connection):
    if N <= 0:
        print("Số phân vùng phải lớn hơn 0")
        return
    
    delta = 5.0 / N
    RPREFIX = 'range_part'
    
    with connection.cursor() as cur:
        cur.execute("""
            SELECT tablename 
            FROM pg_tables 
            WHERE tablename LIKE %s;
        """, (N + '%',))
        old_tables = cur.fetchall()
        
        for (table_name,) in old_tables:
            cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
        
        for i in range(N):
            minRange = i * delta
            maxRange = minRange + delta
            table_name = N + str(i)
            
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
                    SELECT userid, movieid, rating FROM {tablename}
                    WHERE rating >= %s AND rating <= %s;
                """
                params = (minRange, maxRange)
            else:
                sql = f"""
                    INSERT INTO {table_name} (userid, movieid, rating)
                    SELECT userid, movieid, rating FROM {tablename}
                    WHERE rating > %s AND rating <= %s;
                """
                params = (minRange, maxRange)
            
            cur.execute(sql, params)
    
    connection.commit()

def roundrobinpartition(tablename, N ,connection):
    if N <= 0:
        print("Số phân vùng phải lớn hơn 0")
        return
    cur = connection.cursor()
    PREFIX = 'rrobin_part'
    for i in range(N):
        table_name = PREFIX + str(i)
        cur.execute("DROP TABLE IF EXISTS " + table_name + ";")
        cur.execute("CREATE TABLE " + table_name + " (userid integer, movieid integer, rating float);")
        cur.execute(
            "INSERT INTO " + table_name + " (userid, movieid, rating) "
            "SELECT userid, movieid, rating FROM (SELECT userid, movieid, rating, ROW_NUMBER() OVER () as rnum FROM " + tablename + ") as temp "
            "WHERE mod(temp.rnum - 1, %s) = %s;", (N, i))
    cur.close()
    connection.commit()

def roundrobininsert(ratingstablename, userid, itemid, rating,connection):
    cur = connection.cursor()
    PREFIX = 'rrobin_part'
    cur.execute("INSERT INTO " + ratingstablename + "(userid, movieid, rating) VALUES (%s, %s, %s);", (userid, itemid, rating))
    cur.execute("SELECT count(*) FROM " + ratingstablename + ";")
    total_rows = cur.fetchone()[0]
    N = count_partitions(connection, PREFIX)
    if N == 0:
        print("Không có bảng phân vùng round-robin nào tồn tại")
        cur.close()
        return
    index = (total_rows - 1) % N
    table_name = PREFIX + str(index)
    cur.execute("INSERT INTO " + table_name + "(userid, movieid, rating) VALUES (%s, %s, %s);", (userid, itemid, rating))
    cur.close()
    connection.commit()

def rangeinsert(tablename, userid, itemid, rating,connection):
    cur = connection.cursor()
    PREFIX = 'range_part'
    N = count_partitions(connection, PREFIX)
    if N == 0:
        print("Không có bảng phân vùng range nào tồn tại")
        cur.close()
        return
    delta = 5.0 / N
    index = int(rating / delta)
    if rating % delta == 0 and index != 0:
        index -= 1
    table_name = PREFIX + str(index)
    cur.execute("INSERT INTO " + table_name + "(userid, movieid, rating) VALUES (%s, %s, %s);", (userid, itemid, rating))
    cur.close()
    connection.commit()

def count_partitions(connection, prefix):
    cur = connection.cursor()
    cur.execute("SELECT count(*) FROM pg_stat_user_tables WHERE relname LIKE %s;", (prefix + '%',))
    count = cur.fetchone()[0]
    cur.close()
    return count

def main():
    create_db('csdlpt')
    con = getopenconnection()
    if con is None:
        print("Không thể kết nối đến database. Thoát chương trình.")
        return

    try:
        loadratings(con, 'ratings', 'data/ratings.dat')
        rangepartition(con, 'ratings', 4)
        roundrobinpartition(con, 'ratings', 5)
        roundrobininsert(con, 'ratings', 1, 1, 4.5)
        rangeinsert(con, 'ratings', 2, 2, 3.0)
    except Exception as e:
        print("Lỗi trong quá trình xử lý:", e)
    finally:
        con.close()
        print("Đã đóng kết nối đến database.")

if __name__ == '__main__':
    main()