#!/opt/local/bin/python
import sys, re, string
import urllib2
from bs4 import BeautifulSoup as bs
import bs4
from nltk import word_tokenize
from datetime import *


def splitRatingAndNotes(contentRating):
    """
    """
    m = re.search('(.*)\s+\((.*)\).*',contentRating)
    if m:
        try:
            rating = m.group(1).strip()
        except: pass
        try:
            ratingnotes = m.group(2).strip()
        except: pass
    else:
        rating = contentRating.strip()
        ratingnotes = ''
    return rating,ratingnotes


def stripPunct(to_translate, translate_to=u''):
    """
    """
    not_letters_or_digits = u'!"#%\'()*+,-./:;<=>?@[\]^_`{|}~'
    translate_table = dict((ord(char), translate_to) for char in not_letters_or_digits)
    return to_translate.translate(translate_table)


def get_parent_text(elem):
    """
    """
    parentItems = []
    for item in elem.children:
        if isinstance(item,bs4.element.NavigableString):
            parentItems.append(item)
    justParentText = "".join(parentItems)
    return justParentText.strip()


def getTheSoup(url,bsparser='lxml'):
    """                                                                                                                                                     
    """
    try:
        res = urllib2.urlopen(url)
    except (urllib2.URLError, urllib2.HTTPError):
        return None,bs('',bsparser)
    soup = bs(res.read(),bsparser)
    return soup


def resolveURL(url):
    """
    """
    try:
        res = urllib2.urlopen(url)
    except:
        return None
    return res.geturl()


"""
Move these into an RT class
"""


def makeMovieSearchURLRT(movie):
    """
    Make search URL for RT website.
    The movie argument is and array like: 
    [Array]([String]Year,[Year]Title)
    """
    base_url = 'http://www.rottentomatoes.com'
    base_search_url = base_url + '/search/?search='
    movie_title = unicode(movie[1])
    movie_title_words = word_tokenize(stripPunct(movie_title))
    movie_year  = movie[0]
    # construct search url
    search_url = base_search_url
    for word in movie_title_words:
        search_url = search_url + word.lower() + '+'
    search_url = search_url+str(movie[0])

    return search_url


def getMovieURLRT(movie):
    """ 
    Returns the RT movie page URL by sending a simple query to the rottentomatoes website.

    * Using the following simple strategy the subroutine returns ~93% of the titles correctly 
    based on a sample of ~3500 movie titles between the year 2000 and 2014 as listed on the 
    film_in_year pages on Wikipedia. The majority of those not found have different years for
    the movie as reported by RT compared to Wikipedia.

    Three classes of results from query:
    1. Query lands directly on movie page: identify by 
    Case identified by: The h1 heading in the "main_container" divider has class "title".
    Action in this case: simply return the url of the page together with movie year and title.

    2. Query returns a list of relevant matches.
    Case identified by: The h1 heading in the "main_container" divider contains the phrase 
    "Search Results for".
    Action in this case: Go through the list of movie results; compare the queried year
    against the year of the search result, return when years match. If not year matches 
    in search results, print error string: "getMovieURLRT.Error.movieYear".

    3. Query returns no: identify 
    Case identified by: The h1 heading in the "main_container" divider contains the phrase
    "Sorry, no results found for"
    Action in this case: return an error string containing the text "getMovieURLRT.Error".
    """

    base_url = 'http://www.rottentomatoes.com'
    
    search_url = makeMovieSearchURLRT(movie)
    queryPageSoup = getTheSoup(search_url)


    try:
        divMainContainer = queryPageSoup.find("div",attrs={"id":"main_container"})
        h1MainContainer  = divMainContainer.find("h1")
    except:
        print 'getMovieURLRT.Error.h1MainContainer'
        return ''


    # directly lands on movie page, return url
    if h1MainContainer.has_attr('class'):
        if "title" in h1MainContainer.attrs['class']:
            url = resolveURL(search_url)
            return url

    # otherwise inspect list of search results if found
    if "search" in h1MainContainer.get_text().lower():
        try:
            liMovies = divMainContainer.find("div",attrs={"id":"results_all_tab"}) \
                .find("ul",attrs={"id":"movie_results_ul"}) \
                .findAll("li")
        except:
            print 'getMovieURLRT.Error.liMovies'
            return ''


        for liMovie in liMovies:
            try:
                divMovieHead = liMovie.find("div",attrs={"class":"media-body"}) \
                    .find("div",attrs={"class":"media-heading"})
                movieAnchor = divMovieHead.find("a",attrs={"class":"articleLink"})
                movieSpanYear = divMovieHead.find("span",attrs={"class":"movie_year"})
                if movieSpanYear: 
                    m = re.search("(\d{4})",movieSpanYear.get_text())
                    if m:
                        movieYear = int(m.group(1))
                        if movieYear == int(movie[0]):
                            movieURL = base_url + movieAnchor.attrs['href']
                            return movieURL
            except:
                print 'getMovieURLRT.Error.movieYear'
                return ''
        print 'getMovieURLRT.Error.movieYear.NoMatch'

    else:
        print 'getMovieURLRT.Error.unexpectedQueryResult'
        return ''


def getMovieMetaDataRT(moviePageSoup,logfile=None,logging=False,quiet=True):
    """
    Scrape movie meta data from the RT site, return in a dictionary
    data structure.

    RT uses some of the Schemas from schema.org which make parsing easier by querying
    itemscope and itemprop, for example.  
    """

    soup = moviePageSoup
    base_url = 'http://www.rottentomatoes.com'

    divScorePanel = None
    divTabContent = None
    divAllCriticsNumbers = None
    spanAllCriticsRatingValue = None
    divTabContent = None
    divTopCriticsNumbers = None
    spanTopCriticsRatingValue = None
    divAllCriticsNumbers = None
    pCriticConsensus = None
    divMovieInfo = None
    divMovieSynopsis = None
    spanMovieSynopsisRemaining = None
    movieSynopsisRemaining = None
    movieSynopsis = None
    divMovieInfo = None
    divMovieTable = None
    tdContentRating = None
    contentRating = None
    divMovieTable = None
    spansGenre = None
    divMovieTable = None
    tdDatePublished = None
    divMovieInfo = None
    spanProductionCompany = None
    divMovieInfo = None
    timeDuration = None

    rating = None         
    ratingnotes = None    
    rtmeterall = None     
    rtmetertop = None     
    criticConsensus = None
    movieSynopsis = None  
    genres = None         
    releasedate = None    
    studio = None         
    runtime = None        

    # Parse the XML with BeautifulSoup
    try:
        divScorePanel = soup.find("div",attrs={"id":"scorePanel"})
        if divScorePanel: divTabContent = divScorePanel.find("div",attrs={"class":"tab-content"})
        if divTabContent: divAllCriticsNumbers = divTabContent.find("div",attrs={"id":"all-critics-numbers"})
        if divAllCriticsNumbers: 
            spanAllCriticsRatingValue = divAllCriticsNumbers.find("span",attrs={"itemprop":"ratingValue"})
        if spanAllCriticsRatingValue:
            rtmeterall = spanAllCriticsRatingValue.get_text()
        if divTabContent: 
            divTopCriticsNumbers = divTabContent.find("div",attrs={"id":"top-critics-numbers"})
        if divTopCriticsNumbers:
            spanTopCriticsRatingValue = divTopCriticsNumbers.find("span",attrs={"itemprop":"ratingValue"})
        if spanTopCriticsRatingValue:
            rtmetertop = spanTopCriticsRatingValue.get_text()
        if divAllCriticsNumbers:
            pCriticConsensus = divAllCriticsNumbers.find("p",attrs={"class":"critic_consensus"})
        if pCriticConsensus: criticConsensus = get_parent_text(pCriticConsensus)
        divMovieInfo = soup.find("div",attrs={"class":"movie_info"})
        if divMovieInfo: 
            divMovieSynopsis = divMovieInfo.find("div",attrs={"id":"movieSynopsis"})
        if divMovieSynopsis:
            movieSynopsis = divMovieSynopsis.get_text()
            spanMovieSynopsisRemaining = divMovieSynopsis \
                .find("span",attrs={"id":"movieSynopsisRemaining"})
        if spanMovieSynopsisRemaining:
            movieSynopsisRemaining = spanMovieSynopsisRemaining.get_text()
            if movieSynopsisRemaining:movieSynopsis += movieSynopsisRemaining

        if divMovieInfo: divMovieTable   = divMovieInfo.find("table")
        if divMovieTable: tdContentRating = divMovieTable.find("td",attrs={"itemprop":"contentRating"})
        if tdContentRating: contentRating = tdContentRating.get_text()
        if contentRating: 
            rating,ratingnotes = splitRatingAndNotes(contentRating)
        else:
            rating,ratingnotes = "",""

        if divMovieTable: spansGenre = divMovieTable.findAll("span",attrs={"itemprop":"genre"})
        if spansGenre: genres = ",".join([span.get_text() for span in spansGenre])

        if divMovieTable: tdDatePublished = divMovieTable.find("td",attrs={"itemprop":"datePublished"})
        if tdDatePublished: releasedate = tdDatePublished.get_text().strip()
            
        if divMovieInfo: spanProductionCompany = divMovieInfo.find("span",attrs={"itemprop":"productionCompany"})
        if spanProductionCompany: studio = spanProductionCompany.get_text()
        
        if divMovieInfo: timeDuration =divMovieInfo.find("time",attrs={"itemprop":"duration"})
        if timeDuration: runtime = timeDuration['datetime']
        
    except:
        print 'getMovieMetaDataRT.Error.beautifulsoup4'
        pass

    tdWrittenBy = None
    trWrittenBy = None
    aWriters = []
    writers = []
    
    try:
        if divMovieInfo: tdWrittenBy = divMovieInfo.find("td",string="Written By:")
        if tdWrittenBy: trWrittenBy = tdWrittenBy.parent
        if trWrittenBy: aWriters = trWrittenBy.findAll("a")
        for a in aWriters: writers.append([a.get_text().strip(),base_url+a['href']])
    except:
        print 'getMovieMetaDataRT.Error.beautifulsoup4.writers'
        pass
    
    directors = []
    actors = []
    
    try:
        people = soup.findAll(attrs={"itemtype":"http://schema.org/Person"})
        for person in people:

            if person.has_attr("itemprop"): job = job = person['itemprop']
            else: job = ""

            nameItems = person.findAll(attrs={"itemprop":"name"})

            for nameItem in nameItems:
                if job=="actors":
                    a = person.find(attrs={"itemprop":"url"})
                elif job=="director":
                    a = nameItem.parent

                if a.has_attr("href"): rt_url = base_url+a['href']
                else: rt_url = ''

                if(job == "director"):
                    directors.append([nameItem.get_text().strip(),rt_url])

                if(job == "actors"):
                    if nameItem.has_attr('title'): name = nameItem['title'].strip()
                    else: name = nameItem.get_text().strip()

                    role = ""
                    spanRole = person.find("span",attrs={"class":"characters"})
                    if spanRole:
                        if spanRole.has_attr('title'):
                            role = spanRole['title'].strip()
                        else:
                            m = re.search("as (.*)",spanRole.get_text())
                            if m: role = m.group(1).strip()
                    actors.append([name,rt_url,role])
    except:
        print 'getMovieMetaDataRT.Error.beautifulsoup4.people'
        pass


    # load parsed data into dictionary
    metaData = {}
    keys = ['rating','ratingnotes','rtmeterall','rtmetertop','criticconsensus',
            'synopsis','genres','releasedate','studio','runtime']
    for key in keys:
        metaData[key] = ''

    if rating:          metaData['rating'] = rating
    if ratingnotes:     metaData['ratingnotes'] = ratingnotes
    if rtmeterall:      metaData['rtmeterall'] = rtmeterall
    if rtmetertop:      metaData['rtmetertop'] = rtmetertop
    if criticConsensus: metaData['criticconsensus'] = criticConsensus
    if movieSynopsis:   metaData['synopsis'] = movieSynopsis
    if genres:          metaData['genres'] = genres
    if releasedate:     metaData['releasedate'] = releasedate
    if studio:          metaData['studio'] = studio
    if runtime:         metaData['runtime'] = runtime    
    metaData['writers'] = writers
    metaData['actors'] = actors
    metaData['directors'] = directors

    logmsg = u"""[{0},{1},{2}]"""
    msg = logmsg.format(datetime.now().isoformat(),'getMovieMetaDataRT','Success')
    if(logging): logfile.write(msg + "\n")
    if(not quiet): print msg

    return metaData


def getMovieReviewDataRT(moviepage_url):
    """
    """

    base_url = "http://www.rottentomatoes.com"
    review_link = moviepage_url+'reviews/'
    soup = getTheSoup(review_link)
    
    divReviews = None
    pageInfo = None
    pageInfoText = None
    numPages = 0

    # parse the XML with BeautifulSoup
    try:
        divReviews = soup.find("div",attrs={"id":"reviews"})
        if divReviews: pageInfo = divReviews.find("span",attrs={"class":"pageInfo"})
        if pageInfo: pageInfoText = pageInfo.get_text()
        if pageInfoText: m = re.search('\s*Page\s+(\d+)\s*of\s*(\d+)',pageInfoText)
        numPages = 0
        if m: 
            try: 
                onPage = int(m.group(1))
                numPages = int(m.group(2))
            except:
                print 'getMovieReviewDataRT.Error.pageInfoText.regexp'
                pass
    except:
        print 'getMovieReviewDataRT.Error.pageInfo'
        pass
    
    #print "numPages = ", numPages
    theReviews = []
    onPage = 1


    for page in range(numPages):

        divReviews = None
        divReviewRows = []

        try:
            divReviews = soup.find('div',attrs={"class":"review_table"})
            if divReviews: 
                divReviewRows = divReviews.findAll('div',attrs={"class":"review_table_row"})

            for divReviewRow in divReviewRows:

                divTopCritic  = divReviewRow.find("div",attrs={"class":"top_critic"})
                topCritic = 'Top Critic' in divTopCritic.get_text().strip()

                divCriticName = divReviewRow.find("div",attrs={"class":"critic_name"})
                if divCriticName.find("a"): 
                    aCriticName = divCriticName.find("a")
                    criticName = aCriticName.get_text().strip()
                    if aCriticName.has_attr("href"): 
                        critic_url = aCriticName['href']
                else: 
                    criticName = ''
                    critic_url = ''
                    
                
                if divCriticName.find("em"): 
                    criticSource  = divCriticName.find("em").get_text()
                else: criticSource = ''
                
                divReviewContainer = divReviewRow.find("div",attrs={"class":"review_container"})
                divReviewIcon = divReviewContainer.find("div",attrs={"class":"review_icon"})
                fresh = 'fresh' in divReviewIcon.attrs['class']
                
                divReviewDate = divReviewContainer.find("div",attrs={"class":"review_date"})
                if divReviewDate: reviewDate = divReviewDate.get_text().strip()
                else: reviewDate = ''

                divTheReview = divReviewContainer.find("div",attrs={"class":"the_review"})
                blurb = divTheReview.get_text()

                if divReviewContainer.find("a"):
                    aDivReviewContainer= divReviewContainer.find("a")
                    review_url = aDivReviewContainer['href']
                else: review_url = ''

                #print u"""{0:<40s}{1:<40s}{2:<40s}{3:<6s}{4:<6s}{5:<40s}""" \
                #    .format(criticName,criticSource,critic_url,str(topCritic),str(fresh),review_url)
                critic_url = critic_url

                aReview = {}
                aReview['criticname'] = criticName
                aReview['criticsource'] = criticSource
                aReview['criticurl'] = critic_url
                aReview['reviewurl'] = review_url
                aReview['fresh'] = fresh
                aReview['topcritic'] = topCritic
                aReview['blurb'] = blurb
                theReviews.append(aReview)


        except:
            print 'Error: onPage = ',onPage
            pass

        onPage += 1
        if onPage <= numPages:
            new_url = review_link + '?page=' + str(onPage)
            #print new_url
            try:
                res = urllib2.urlopen(new_url)
            except (urllib2.URLError, urllib2.HTTPError):
                return 'getMovieReviewLinksRT.urlopen'
            soup = bs(res.read(),'lxml')

    return theReviews
