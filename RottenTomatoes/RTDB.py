"""
Mostly wrappers for SQL queries on a database to store
scraped Rotten Tomatoes data.
"""

import sys
from datetime import *
from RottenTomatoes import *
import sqlite3 as lite

def escapeQuotes(string):
    string = re.sub(u"\"",u"\"\"",unicode(string))
    string = re.sub(u"\'",u"\'\'",unicode(string))
    return string


def readMovieList(movie_list_file):
    movie_list = open(movie_list_file)
    movies = []
    for line in movie_list:
        year = line.split()[0]
        title = line.split(year + '\t')[1].strip()
        movies.append((year,title))
    return movies


def connectDB(sqlite3_db_file_name):
    con = lite.connect(sqlite3_db_file_name)
    return con


def updateTableRowKeyValueString(con,table,rowid,key,stringValue):
    cur = con.cursor()
    sql_ = u"""update {0} set {1} =  "{2}" where rowid = {3};"""
    sqlcmd = sql_.format(table,key,stringValue,rowid)    
    try:
        results = cur.execute(sqlcmd).fetchall()
    except:
        print 'Error: ' + sqlcmd
        results = None
    con.commit()
    return result


def trySqlcmdFetchall(con,sqlcmd,logfile=None,quiet=True,logging=False):
    cur = con.cursor()
    try: 
        results = cur.execute(sqlcmd).fetchall()
        statCode = 'Success'
    except:
        results = None
        statCode = 'Error'
    logmsg = u"""[{0},{1},{2}]"""
    msg = logmsg.format(datetime.now().isoformat(),sqlcmd,statCode)
    if(logging): logfile.write(msg+"\n")
    if(not quiet): print msg
    return results


def trySqlcmdCommit(con,sqlcmd,logfile=None,quiet=True,logging=False):
    cur = con.cursor()
    try:
        cur.execute(sqlcmd)
        statCode = 'Success'
        con.commit()
        rowid = cur.lastrowid
    except:
        if(not quiet): print 'ERROR: ' + sqlcmd
        statCode = 'Error'
        rowid = 0
    logmsg = u"""[{0},{1},{2}]"""
    msg = logmsg.format(datetime.now().isoformat(),sqlcmd,statCode)
    if(logging): logfile.write(msg+"\n")
    if(not quiet): print msg
    return rowid

def logRTDB(logfile,op,msg,quiet=True,doWrite=True):                                             
    """                                                                                          
    """                                                                                          
    msg = u"""[ {0}, {1}, {2} ]""".format(datetime.now().isoformat(),op,msg)                     
    if doWrite: logfile.write(msg + "\n")                                                        
    if not quiet: print msg                                                                      
    return msg


def getMovieURLAndIdFromDB(con,movie):                                                                 
    """                                                                                                
    """                                                                                                
    sqlGetMovieInfo_ = u"""select rowid,rtmovieurl from movies where year="{0}" and title ="{1}";"""   
    sqlcmd = sqlGetMovieInfo_.format(movie[0],movie[1])                                                
    results = trySqlcmdFetchall(con,sqlcmd)                                                            
    if not results:                                                                                    
        print 'Error'                                                                                  
        return None                                                                                    
    movieid,url = results[0]                                                                           
    return (movieid,url)


def stripSearchStringFromMovieURL(con,movie):
    """
    """
    id,url = getMovieURLAndIdFromDB(con,movie)
    new_url = None
    if url:
        split_url = url.split('?search')
        new_url = split_url[0]
    return id,new_url


"""
Routines to update RT DB
"""


def updateMoviesRTURL_StripSearchString(con,movie):
    """
    """
    id,url = getMovieURLAndIdFromDB(con,movie)
    id,url_strip = stripSearchStringFromMovieURL(con,movie)

    if url_strip and url != url_strip:
        updateMoviesRTURL(con,id,url_strip)
    
        

def updateMoviesRTURL(con,movieid,rturl):
    """                                                                                          
    """                                                                                          
    sqlUpdateMovieURL_ = u"""update movies set rtmovieurl = "{0}" where rowid = "{1}";"""        
    sqlcmd = sqlUpdateMovieURL_.format(rturl,movieid)                                            
    rowid= trySqlcmdCommit(con,sqlcmd)                                                           
    return rowid


def updateMovieMetaDataRTDB(con,movieid,metaData,logfile=None,logging=False,quiet=True):
    """
    Scrape RT URL for movie meta data.
    Insert into sqlite3 tables.
    """
    sqlGetMovieData_   = u"""select * from movies where rowid = {0};"""
    sqlUpdateMoviesKV_ = u"""update movies set {0} = '{1}' where rowid = {2};"""

    sqlcmd = sqlGetMovieData_.format(movieid)
    results = trySqlcmdFetchall(con,sqlcmd)
    if not results:
        return 'Error'
    row = results[0]
    title,year,releasedate,rtmeterall,rtmetertop,criticconsensus,runtime,rating, \
        ratingnotes,medium,version,genres,studio,synopsis,url = row

    vars = ["releasedate","rtmeterall","rtmetertop","criticconsensus","runtime","rating", \
                "ratingnotes","genres","studio","synopsis"]
    varlist = [var for var in vars if not eval(var)]
    for var in varlist:
        value = escapeQuotes(unicode(eval(u"""metaData['{0}']""".format(var))))
        
        sqlcmd = sqlUpdateMoviesKV_.format(var,value,movieid)
        trySqlcmdCommit(con,sqlcmd)


    for director in metaData['directors']:
        updateDirectorRTDB(con,movieid,director)
    
    for writer in metaData['writers']:
        updateWritersRTDB(con,movieid,writer)
        
    for actor in metaData['actors']:
        updateActorAndCharacterRTDB(con,movieid,actor)


def updatePersonRTDB(con,credited,rturl):
    """
    """
    sqlSelectRTUrlPeople_ = u"""select personid, rturl from people where rturl = "{0}";"""
    sqlUpdateRTUrlPeople_ = u"""insert into people (credited,rturl) values ("{0}","{1}");"""

    sqlcmd = sqlSelectRTUrlPeople_.format(rturl)
    results = trySqlcmdFetchall(con,sqlcmd)
    if results: 
        personid = results[0][0]
    else:
        sqlcmd = sqlUpdateRTUrlPeople_.format(credited,rturl)
        personid = trySqlcmdCommit(con,sqlcmd)

    return personid


def updateDirectorRTDB(con,movieid,director):
    """
    """
    sqlSelectDirectors_ = u"""select * from directors where personid = {0} and movieid = {1};"""
    sqlInsertDirectors_ = u"""insert into directors (personid,movieid) values ({0},{1});"""

    credited = escapeQuotes(director[0])
    rturl = director[1]
    personid = updatePersonRTDB(con,credited,rturl)

    sqlcmd = sqlSelectDirectors_.format(personid,movieid)
    results = trySqlcmdFetchall(con,sqlcmd)
    if results: print "Director alread exists in DB"
    else:
        sqlcmd = sqlInsertDirectors_.format(personid,movieid)
        rowid = trySqlcmdCommit(con,sqlcmd)


def updateWritersRTDB(con,movieid,writer):
    """
    """
    sqlSelectWriters_ = u"""select * from writers where personid = {0} and movieid = {1};"""
    sqlInsertWriters_ = u"""insert into writers (personid,movieid) values ({0},{1});"""
    
    credited = escapeQuotes(writer[0])
    rturl    = writer[1]
    personid = updatePersonRTDB(con,credited,rturl)
        
    sqlcmd = sqlSelectWriters_.format(personid,movieid)
    results = trySqlcmdFetchall(con,sqlcmd)
    if results: print "Writer alread exists in DB"
    else:
        sqlcmd = sqlInsertWriters_.format(personid,movieid)
        rowid = trySqlcmdCommit(con,sqlcmd)


def updateActorAndCharacterRTDB(con,movieid,actor):
    """
    """
    sqlSelectActors_ = u"""select rowid,* from actors where personid = {0} and movieid = {1};"""
    sqlInsertActors_ = u"""insert into actors (personid,movieid) values ({0},{1});"""
    sqlSelectChar_ = u"""select * from characters where actorid = {0} and movieid = {1} and name = "{2}";"""
    sqlInsertChar_ = u"""insert into characters (actorid,movieid,name) values ({0},{1},"{2}");"""

    credited = escapeQuotes(actor[0])
    rturl = actor[1]
    personid = updatePersonRTDB(con,credited,rturl)

    sqlcmd = sqlSelectActors_.format(personid,movieid)
    results = trySqlcmdFetchall(con,sqlcmd)
    if results: 
        print "Actor alread exists in DB"
        row = results[0]
        actorid = row[0]
    else:
        sqlcmd = sqlInsertActors_.format(personid,movieid)
        actorid = trySqlcmdCommit(con,sqlcmd)
        
    name = escapeQuotes(actor[2])
    if not name:
        print "Character name not available"
        return
    sqlcmd = sqlSelectChar_.format(actorid,movieid,name)
    results = trySqlcmdFetchall(con,sqlcmd)
    if results: print "Character already exist in DB"
    else:
        sqlcmd = sqlInsertChar_.format(actorid,movieid,name)
        rowid = trySqlcmdCommit(con,sqlcmd)


def updateReviewsourceRTDB(con,sourcename,url="",logfile=None,logging=False,quiet=True):
    """
    """
    sqlSelectReviewsource_=u"""select sourceid from reviewsources where name = "{0}";"""
    sqlInsertReviewsource_=u"""insert into reviewsources (name,url) values ("{0}","{1}");"""

    sqlcmd = sqlSelectReviewsource_.format(sourcename)
    results = trySqlcmdFetchall(con,sqlcmd)
    if results:
        row = results[0]
        sourceid = row[0]
    else:
        sqlcmd = sqlInsertReviewsource_.format(sourcename,url)
        sourceid = trySqlcmdCommit(con,sqlcmd)        
    return sourceid


def updateCriticRTDB(con,personid,sourceid,topcritic=0,logfile=None,logging=False,quiet=True):
    """
    """
    sqlSelectCritic_ = \
        u"""select rowid,personid,sourceid,topcritic from critics where personid ={0} and sourceid = {1};"""
    sqlInsertCritic_ = u"""insert into critics (personid,sourceid,topcritic) values ({0},{1},{2});"""
    
    sqlcmd = sqlSelectCritic_.format(personid,sourceid)
    results = trySqlcmdFetchall(con,sqlcmd)

    if results:
        criticid = results[0][0]
    else:
        sqlcmd = sqlInsertCritic_.format(personid,sourceid,int(topcritic))
        criticid = trySqlcmdCommit(con,sqlcmd)

    return criticid



def updateReviewRTDB(con,movieid,review,logfile=None,logging=False,quiet=True):
    """
    TO-DO:    
    
    Check if there is a review with criticid,movieid pair.
    If so, update blank entries. If not, insert new record.
    """

    getReviewidUnique_ = \
        u"""select rowid from reviews where criticid = {0} and movieid = {1} and reviewurl = "{2}";"""
    insertReview_ = \
        u"""insert into reviews (criticid,movieid,reviewurl,fresh,blurb,date) values ({0},{1},"{2}",{3},"{4}","{5}");"""

    base_url = "http://www.rottentomatoes.com"

    credited = escapeQuotes(review['criticname'])
    rturl = base_url + review['criticurl']
    source = escapeQuotes(review['criticsource'])
    topcritic = review['topcritic']
    
    personid = 0
    sourceid = 0
    criticid = 0

    if credited or rturl: 
        personid = updatePersonRTDB(con,credited,rturl)

    if source: 
        sourceid = updateReviewsourceRTDB(con,source)    
        
    if personid != 0 or sourceid != 0: 
        criticid = updateCriticRTDB(con,personid,sourceid,topcritic)
        
    results = []
    if criticid != 0:
        reviewurl = review['reviewurl']
        sqlcmd = getReviewidUnique_.format(criticid,movieid,reviewurl)
        results = trySqlcmdFetchall(con,sqlcmd)

    if not results:
        fresh = int(review['fresh'])
        blurb = escapeQuotes(review['blurb'])
        pubdate = review['pubdate']
        sqlcmd = insertReview_.format(criticid,movieid,reviewurl,fresh,blurb,pubdate)
        results = trySqlcmdCommit(con,sqlcmd)
    else:
        print "sqlUpdateReviewRTDB. Already a review for this movie,critic pair."

    return 0

def updateReviewsRTDB(con,movieid,reviewData,logfile=None,logging=False,quiet=True):
    """
    """
    for review in reviewData:        
        reviewid = updateReviewRTDB(con,movieid,review)


"""
Routines to populate RT DB
"""


def populateActorsAndCharacters(con,movie):
    """
    """
    result = getMovieURLAndIdFromDB(con,movie)
    if result:
        movieid,url = result
        if not url:
            print 'Error: getMovieURLAndIdFromDB'
            return
    else:
        print 'Error: getMovieURLAndIdFromDB'
        return

    result = getMovieMetaDataRT(url)
    if result:
        exitCode,metaData = result
        if not metaData:
            print 'Error.getMovieMetaDataRT'
            return
    else:
        print 'Error.getMovieMetaDataRT'
        return

    for actor in metaData['actors']:
        updateActorAndCharacterRTDB(con,movieid,actor)


def populateMovieMetaData(con,movie):
    """
    """
    result = getMovieURLAndIdFromDB(con,movie)
    if result: 
        movieid,url = result
        if not url:
            print 'Error: getMovieURLAndIdFromDB'
            return
    else:
        print 'Error: getMovieURLAndIdFromDB'
        return

    result = getMovieMetaDataRT(url)
    if result:
        exitCode,metaData = result
        if not metaData:
            print 'Error.getMovieMetaDataRT'
            return
    else:
        print 'Error.getMovieMetaDataRT'
        return
    updateMovieMetaDataRTDB(con,movieid,metaData)


def populateTitleYear(con,movieList):                                      
    """
    """
    cur = con.cursor()                                                     
    sql_ =  """insert into movies (year, title) values ({0}, "{1}");"""    
    for movie in movieList:                                                
        sqlcmd = sql_.format(movie[0],movie[1])                            
        try:                                                               
            result = cur.execute(sqlcmd)                                   
        except:                                                            
            print 'ERROR: ' + sqlcmd                                       
    con.commit()                                                           


def populateReviewsRTDB(con,movieList,logfname="populateReviews.log",quiet=True):
    """
    """
    myName = "populateReviewsRTDB"
    logfile = open(logfname,"a")
    sqlMovieidRTURL_ = u"""select rowid,rtmovieurl from movies where year = {0} and title = "{1}";"""

    for movie in movieList:
        
        if not quiet: print u"""{1},{0}""".format(movie[0],movie[1])
        sqlcmd = sqlMovieidRTURL_.format(movie[0],movie[1])
        results = trySqlcmdFetchall(con,sqlcmd)
        if not results:
            msg = logRTDB(logfile,myName,'Error: '+sqlcmd,quiet=quiet)
            continue

        row = results[0]
        movieid = row[0]
        rturl = row[1]

        if rturl:
            reviewData = getMovieReviewDataRT(rturl)
            updateReviewsRTDB(con,movieid,reviewData)
            
    
        
def populateMoviesUrlRTDB(con,movieList,logfname="populateRTURL.log",quiet=True):
    """
    """
    myName = "populateMovieRTURL"    
    logfile = open(logfname,"a")
    sqlGetRowidRTURL_ = u"""select rowid,rtmovieurl from movies where year = {0} and title = "{1}";"""

    for movie in movieList:

        if not quiet: print u"""{1},{0}""".format(movie[0],movie[1])

        sqlcmd = sqlGetRowidRTURL_.format(movie[0],movie[1])
        results = trySqlcmdFetchall(con,sqlcmd)
        if not results:
            msg = logRTDB(logfile,myName,'Error: '+sqlcmd,quiet=quiet)
            continue

        row = results[0]
        movieid = row[0]
        rturl = row[1]
        if not rturl:
            url = getMovieURLRT(movie)
            if url: 
                rowid = updateMoviesRTURL(con,movieid,url)
                msg = logRTDB(logfile,myName,'Success: '+sqlcmd,quiet=quiet)
            else:
                msg = logRTDB(logfile,myName,u"""Error: getMovieURLRT {1},{0}""".format(movie[0],movie[1]),quiet=quiet)

    logfile.close()

