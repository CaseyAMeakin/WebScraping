import sys
from RottenTomatoes import *
import sqlite3 as lite

# Populate list of movie data: [Array](year, title) 
def readMovieList(movie_list_file):
    movie_list = open(movie_list_file)
    movies = []
    for line in movie_list:
        year = line.split()[0]
        title = line.split(year + '\t')[1].strip()
        print year, title
        movies.append((year,title))
    return movies

# Connect to DB return cursor
def connectDB(sqlite3_db_file_name):
    con = lite.connect(sqlite3_db_file_name)
    return con

# Query rows: (cursor, table name, column name, row value)
def getRows(con,tablename,condition):
    cur = con.cursor()
    sqlcmd = '''select * from ''' + tablename + " where " + condition
    print sqlcmd
    cur.execute(sqlcmd)
    return cur.fetchall()


# Insert Title and Year into table "movie"
def populateTitleYear(con,movieList):
    cur = con.cursor()
    sqltmp =  """insert into movies (year, title) values ({0}, "{1}");"""
    for movie in movieList:
        sqlcmd = sqltmp.format(movie[0],movie[1])
        try:
            result = cur.execute(sqlcmd)
        except:
            print 'ERROR: ' + sqlcmd
    con.commit()
