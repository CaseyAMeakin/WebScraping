"""
Mostly wrappers for SQL queries on a database to store
scraped Rotten Tomatoes data.
"""

import sys
from datetime import *
from RottenTomatoes import *
import sqlite3 as lite

def escapeQuotes(string):
    string = re.sub("\"","\"\"",string)
    string = re.sub("\'","\'\'",string)
    return string


# Return list of movie data arrays, each like: [Array](year, title) 
def readMovieList(movie_list_file):
    movie_list = open(movie_list_file)
    movies = []
    for line in movie_list:
        year = line.split()[0]
        title = line.split(year + '\t')[1].strip()
        #print year, title
        movies.append((year,title))
    return movies

# Connect to DB, return connection
def connectDB(sqlite3_db_file_name):
    con = lite.connect(sqlite3_db_file_name)
    return con


# Insert Title and Year into sqlite3 table "movie"
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

# Update a string column for a given row in a sqlite3 database
def updateTableRowKeyValueString(con,table,rowid,key,stringValue):
    cur = con.cursor()
    sql_ = """update {0} set {1} =  "{2}" where rowid = {3};"""
    sqlcmd = sql_.format(table,key,stringValue,rowid)
    
    result = con.execute(sqlcmd)
    con.commit()


def populateRTMetaData(con,movieList,logfname="populateRTMetaData.log"):
    """
    Scrape RT URL for movie meta data.
    Insert into sqlite3 tables.
    """
    logfile=open(logfname,"write")
    logmsg = """[{0},{1},{2}]"""
    
    cur = con.cursor()
    
    # SQLite3 command templates
    sqlGetMovieData_   = """select rowid,* from movies where year = {0} and title = "{1}";"""
    sqlGetPersonID_    = """select rowid from people where rturl = {0};"""
    sqlGetActorID_     = """select rowid from actors where peopleid = {0} and movieid = {1};"""
    sqlGetDirectorID_  = """select rowid from directors where peopleid = {0} and movieid = {1};"""
    sqlGetCharacterID_ = """select rowid from characters where peopleid = {0} and movieid = {1} and name = {2};"""
    
    for movie in movieList:

        sqlGetMovieData = sqlGetMovieData_.format(movie[0],movie[1])
        results = cur.execute(sqlGetMovieData).fetchall()

        if results:
            msg = logmsg.format(datetime.now().isoformat(),sqlGetMovieData,"Success")
            logfile.write(msg+"\n")
            row = results[0]
            movieid,title,year,releasedate,rtmeterall,rtmetertop,criticconsensus,runtime,rating, \
                ratingnotes,medium,version,genres,studio,synopsis,url = row
            
            # Scrape Metadata from RT website
            (exitCode,metaData) = getMovieMetaDataRT(url)
            msg = logmsg.format(datetime.now().isoformat(),"getMovieMetaDataRT("+url+")",exitCode)
            logfile.write(msg+"\n")
            
            if re.search(".*success.*",exitCode.lower()):
                # First, populate meta data in "movies" table
                vars = ["releasedate","rtmeterall","rtmetertop","criticconsensus","runtime","rating", \
                            "ratingnotes","genres","studio","synopsis"]
                varlist = [var for var in vars if not eval(var)]
                for var in varlist:
                    value = escapeQuotes(str(eval("""metaData['{0}']""".format(var))))
                    sqlupdate = """update movies set {0} = '{1}' where rowid = {2};""".format(var,value,movieid)
                    try: 
                        cur.execute(sqlupdate)
                    except:
                        print 'ERROR: ' + sqlupdate
                    con.commit()
                    
                # Populate people-related tables: people, actors, directors, writers, characters
                sqlSelectRTUrlPeople_ = """select personid, rturl from people where rturl = "{0}";"""
                sqlUpdateRTUrlPeople_ = """insert into people (credited,rturl) values ("{0}","{1}");"""
                sqlSelectDirectors_ = """select * from directors where personid = {0} and movieid = {1};"""
                sqlInsertDirectors_ = """insert into directors (personid,movieid) values ({0},{1});"""

                for person in metaData['directors']:
                    # query person in people table by rturl
                    sqlcmd = sqlSelectRTUrlPeople_.format(person[1])
                    try:results = con.execute(sqlcmd).fetchall()                    
                    except: print 'ERROR: ' + sqlcmd
                    # if not in db insert credited name and rturl and get back rowid
                    if not results:
                        credited = escapeQuotes(person[0])
                        rturl = person[1]
                        sqlcmd = sqlUpdateRTUrlPeople_.format(credited,rturl)
                        print sqlcmd
                        personid = None
                        try:
                            cur.execute(sqlcmd)
                            personid = cur.lastworid
                        except:
                            print 'ERROR: ' + sqlcmd
                        con.commit()
                    else:
                        row = results[0]
                        personid = row[0]
                    sqlcmd = sqlSelectDirectors_.format(personid,movieid)
                    try: results = cur.execute(sqlcmd).fetchall()
                    except: print 'ERROR: ' + sqlcmd
                    if not results:
                        sqlcmd = sqlInsertDirectors_.format(personid,movieid)
                        print results
                        print sqlcmd
                        try: results = cur.execute(sqlcmd)
                        except: print 'ERROR: '+ sqlcmd
                    else:
                        print results
                        print "Director already exists in DB"
                    con.commit()

                #for person in metaData['actors']:
                    


        else:
            msg= logmsg.format(datetime.now().isoformat(),sqlGetMovieData,"Error.noSqlResult")
            logfile.write(msg+"\n")

def populateRTURL(con,movieList,logfname="populateRTURL.log"):
    """                                                             
    Scrape RT URL for a movie and populate tables rows.             
    This is specific to a sqlite3 table which has        
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
    logfile = open(logfname,"a")
    cur = con.cursor()
    sqlGetRowid_ = """select rowid from movies where year = {0} and title = "{1}";"""

    for movie in movieList:
        # Get rowid for movie with given title and year
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
    
        # Scrape URL from RT website
        result = getMovieURLRT(movie)
        url = result[2]        

        # Add URL data to sqlite3 table
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

