"""
Mostly wrappers for SQL queries on a database to store
scraped Rotten Tomatoes data.
"""

import sys
from datetime import *
from RottenTomatoes import *
import sqlite3 as lite

# Populate list of movie data: [Array](year, title) 
def readMovieList(movie_list_file):
    movie_list = open(movie_list_file)
    movies = []
    for line in movie_list:
        year = line.split()[0]
        title = line.split(year + '\t')[1].strip()
        #print year, title
        movies.append((year,title))
    return movies

# Connect to DB return cursor
def connectDB(sqlite3_db_file_name):
    con = lite.connect(sqlite3_db_file_name)
    return con


# Insert Title and Year into table "movie"
def populateTitleYear(con,movieList):
    cur = con.cursor()
    sql_ =  """insert into movies (year, title) values ({0}, "{1}");"""
    for movie in movieList:
        sqlcmd = sql_.format(movie[0],movie[1])
        try:
            result = cur.execute(sqlcmd)
        except:
            print 'ERROR: ' + sqlcmd
    con.commit()

# Update a string column for a given row
def updateTableRowKeyValueString(con,table,rowid,key,stringValue):
    cur = con.cursor()
    sql_ = """update {0} set {1} =  "{2}" where rowid = {3};"""
    sqlcmd = sql_.format(table,key,stringValue,rowid)
    
    result = con.execute(sqlcmd)
    con.commit()


def populateRTURL(con,movieList,logfname="populateRTURL.log"):
    """                                                             
    Scrape RT URL for a movie and populate tables rows.             
    This is specific to the "movies" sqlite3 table which has        
    this schema:                                                    
    
    CREATE TABLE movies (                                           
    title text,                                                     
    year int,                                                       
    releasedate text,                                               
    rtmeterall int,                                                 
    rtmetertop int,                                                 
    criticconsensus text,                                           
    runtime int,                                                    
    rating text,                                                    
    ratingnotes text,                                               
    medium text default "",                                         
    version text default "",                                        
    genres text,                                                    
    studio text,                                                    
    synopsis text,                                                  
    rtmovieurl text,                                                
    unique(title,year,medium,version));                             
    """
    cur = con.cursor()
    sqlGetRowid_ = """select rowid from movies where year = {0} and title = "{1}";"""
    logfile = open(logfname,"a")

    for movie in movieList:
        sqlGetRowid = sqlGetRowid_.format(movie[0],movie[1])
        rowid_ = cur.execute(sqlGetRowid).fetchall()
        if len(rowid_) != 1:
            msg = "{0}\t{1}\t{2}\t{3}" \
                .format(movie[0],movie[1],'populateRTURL.Error.rowid',datetime.now().isoformat())
            print msg
            logfile.write(msg+"\n")
            return
        try:
            rowid = rowid_[0][0]
        except:
            msg = "{0}\t{1}\t{2}\t{3}" \
                .format(movie[0],movie[1],'populateRTURL.Error.rowid',datetime.now().isoformat())
            
        result = getMovieURLRT(movie)
        url = result[2]
        if "error" not in url.lower():
            try:
                updateTableRowKeyValueString(con,"movies",rowid,"rtmovieurl",url)
            except:
                msg = "{0}\t{1}\t{2}\t{3}".format(result[0],result[1],'Sqlite3.Error',datetime.now().isoformat())
                print msg
                logfile.write(msg+"\n")
                return

        msg = "{0}\t{1}\t{2}\t{3}".format(result[0],result[1],result[2],datetime.now().isoformat())
        print msg
        logfile.write(msg+"\n")
