import MySQLdb as mdb


def connectMysql(db,dbuser,dbpw):
    """
    """
    try:
        con = mdb.connect(db=db,user=dbuser,passwd=dbpw)
    except:
        print 'Trouble connecting to MySQL db'
        con = None
    return con
        
def tryMysqlFetchall(con,sqlcmd):
    """
    """
    try:
        cur     = con.cursor()
        curcall = cur.execute(sqlcmd)
        query   = cur.fetchall()
    except:
        query    = []
    return query
