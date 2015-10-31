#!/opt/local/bin/python
import sys, re, string
import urllib2
from bs4 import BeautifulSoup as bs
import bs4
from nltk import word_tokenize


def checkFind(item,itemName):
    if not item:
        errorCode = 'getMovieMetaDataRT.Error.' + itemName
        return errorCode,{}

def stripPunct(to_translate, translate_to=u''):
    not_letters_or_digits = u'!"#%\'()*+,-./:;<=>?@[\]^_`{|}~'
    translate_table = dict((ord(char), translate_to) for char in not_letters_or_digits)
    return to_translate.translate(translate_table)

def get_parent_text(elem):
    parentItems = []
    for item in elem.children:
        if isinstance(item,bs4.element.NavigableString):
            parentItems.append(item)
    justParentText = "".join(parentItems)
    return justParentText.strip()


def getMovieURLRT(movie):
    """ 
    Returns the RT movie page URL by sending a simple query to the rottentomatoes website.

    * Using this simple strategy the subroutine returns ~90% of the titles correctly based on a
    sample of ~3500 movie titles between the year 2000 and 2014 as listed on the film_in_year 
    pages on Wikipedia.  The majority of misses are due to the movie page not being the first 
    in the list for case 2 below: fixing this should result in ~98% correct hit rate.  About half
    of the remaining misses has to do with handling of special characters in the name, mostly 
    the dash "-".

    Three classes of results from query:
    1. Query lands directly on movie page: identify by 
    Case identified by: The h1 heading in the "main_container" divider has class "title".
    Action in this case: simply return the url of the page together with movie year and title.

    2. Query returns a list of relevant matches.
    Case identified by: The h1 heading in the "main_container" divider contains the phrase 
    "Search Results for".
    Action in this case: select the first match in the list of movie results as long as the 
    movie year for that entry agrees with the query, otherwise return an error string
    containing the text "getMovieURLRT.Error".

    3. Query returns no: identify 
    Case identified by: The h1 heading in the "main_container" divider contains the phrase
    "Sorry, no results found for"
    Action in this case: return an error string containing the text "getMovieURLRT.Error".
    """

    base_url = 'http://www.rottentomatoes.com'
    base_search_url = base_url + '/search/?search='

    movie_title = unicode(movie[1])
    movie_title_words = word_tokenize(stripPunct(movie_title))
    movie_year  = movie[0]

    # construct query url
    search_url = base_search_url
    for word in movie_title_words:
        search_url = search_url + word.lower() + '+'
    search_url = search_url+str(movie[0])

    # open query url
    try:
        res = urllib2.urlopen(search_url)
    except (urllib2.URLError, urllib2.HTTPError):
        return (movie[0],movie[1],'getMovieURLRT.Error.urlopen')

    # read the page and parse with bs4
    f = res.read()
    resultsSoup = bs(f,'lxml')

    # find main_contents container div and h1 header
    divMainContainer = resultsSoup.find("div",attrs={"id":"main_container"})
    if not divMainContainer: 
        return (movie[0],movie[1],'getMovieURLRT.Error.divMainContainer')
    h1MainContainer  = divMainContainer.find("h1")
    if not h1MainContainer:
        return (movie[0],movie[1],'getMovieURLRT.Error.h1MainContainer')

    # Case 1. directly lands on movie page
    if h1MainContainer.has_attr('class'):
        if "title" in h1MainContainer.attrs['class']:
            return (movie[0],movie[1],res.geturl())

    # Case 2. list of search results
    if "search" in h1MainContainer.get_text().lower():
        divResultsAllTab = divMainContainer.find("div",attrs={"id":"results_all_tab"})
        if not divResultsAllTab:
            return (movie[0],movie[1],'getMovieURLRT.Error.divResultsAllTab')
        ulMovieResults = divResultsAllTab.find("ul",attrs={"id":"movie_results_ul"})
        if not ulMovieResults:
            return (movie[0],movie[1],'getMovieURLRT.Error.ulMovieResults')
        liMovies = ulMovieResults.findAll("li")
        if not liMovies:
            return (movie[0],movie[1],'getMovieURLRT.Error.liMovies')
        divFirstMovieBody = liMovies[0].find("div",attrs={"class":"media-body"})
        if not divFirstMovieBody:
            return (movie[0],movie[1],'getMovieURLRT.Error.divFirstMovieBody')
        divFirstMovieHead = divFirstMovieBody.find("div",attrs={"class":"media-heading"})
        if not divFirstMovieHead:
            return (movie[0],movie[1],'getMovieURLRT.Error.divFirstMovieHead')
        firstMovieAnchor = divFirstMovieHead.find("a",attrs={"class":"articleLink"})
        if not firstMovieAnchor:
            return (movie[0],movie[1],'getMovieURLRT.Error.firstMovieAnchor')
        #firstMovieTitle = firstMovieAnchor.get_text()
        firstMovieSpanYear = divFirstMovieHead.find("span",attrs={"class":"movie_year"})
        if firstMovieSpanYear:
            firstMovieYear = re.search("(\d{4})",firstMovieSpanYear.get_text()).group(0)
            if int(firstMovieYear) != int(movie[0]):
                return (movie[0],movie[1],'getMovieURLRT.Error.firstMovieYearMatch')
        else:
            return (movie[0],movie[1],'getMovieURLRT.Error.firstMovieSpanYear')
        firstMovieURL = base_url + firstMovieAnchor.attrs['href']
        return  (movie[0],movie[1],firstMovieURL)

    # Case 3. query returns no results or some other unaccounted for case
    return (movie[0],movie[1],'getMovieURLRT.Error.NoResults')


def getMovieMetaDataRT(url):
    """
    
    """

    try:
        res = urllib2.urlopen(url)
    except (urllib2.URLError, urllib2.HTTPError):
        return 'getMovieMetaDataRT.Error.urlopen', {}
    soup = bs(res.read(),'lxml')


    divScorePanel = soup.find("div",attrs={"id":"scorePanel"})
    checkFind(divScorePanel,'divScorePanel')

    divTabContent = divScorePanel.find("div",attrs={"class":"tab-content"})
    checkFind(divTabContent,'divTabContent')

    divAllCriticsNumbers = divTabContent.find("div",attrs={"id":"all-critics-numbers"})
    checkFind(divAllCriticsNumbers,'divAllCriticsNumbers')

    spanAllCriticsRatingValue = divAllCriticsNumbers.find("span",attrs={"itemprop":"ratingValue"})
    checkFind(spanAllCriticsRatingValue,'spanAllCriticsRatingValue')

    divTopCriticsNumbers = divTabContent.find("div",attrs={"id":"top-critics-numbers"})
    checkFind(divTopCriticsNumbers,'divTopCriticsNumbers')

    spanTopCriticsRatingValue = divTopCriticsNumbers.find("span",attrs={"itemprop":"ratingValue"})
    checkFind(spanTopCriticsRatingValue,'spanTopCriticsRatingValue')
    
    pCriticConsensus = divTopCriticsNumbers.find("p",attrs={"class":"critic_consensus"})
    checkFind(pCriticConsensus,'pCriticConsensus')

    divMovieInfo = soup.find("div",attrs={"class":"movie_info"})
    checkFind(divMovieInfo,'divMovieInfo')
    
    pMovieSynopsis = divMovieInfo.find("p",attrs={"id":"movieSynopsis"})
    checkFind(pMovieSynopsis,'pMovieSynopsis')

    spanMovieSynopsisRemaining = pMovieSynopsis.find("span",attrs={"id":"movieSynopsisRemaining"})
    checkFind(spanMovieSynopsisRemaining,'spanMovieSynopsisRemaining')

    tableMovieInfo = divMovieInfo.find("table")
    checkFind(tableMovieInfo,'tableMovieInfo')

    tdContentRating = tableMovieInfo.find("td",attrs={"itemprop":"contentRating"})
    checkFind(tdContentRating,'tdContentRating')

    spansGenre = tableMovieInfo.findAll("span",attrs={"itemprop":"genre"})
    checkFind(spansGenre,'spansGenre')

    tdDatePublished = tableMovieInfo.find("td",attrs={"itemprop":"datePublished"})
    checkFind(tdDatePublished,'tdDatePublished')
    
    tdDirector =  tableMovieInfo.find("td",attrs={"itemprop":"director"})
    checkFind(tdDirector,'tdDirector')

    spanProductionCompany = divMovieInfo.find("span",attrs={"itemprop":"productionCompany"})
    checkFind(spanProductionCompany,'spanProductionCompany')

    timeDuration =divMovieInfo.find("time",attrs={"itemprop":"duration"})
    checkFind(timeDuration,'timeDuration')


    # Init metaData dictionary

    metaData = {}


    # Parse elements pulled from page, add to dictionary

    allCriticsRatingValue = spanAllCriticsRatingValue.get_text()
    topCriticsRatingValue = spanTopCriticsRatingValue.get_text()
    criticConsensus = get_parent_text(pCriticConsensus)
    movieSynopsis = get_parent_text(pMovieSynopsis)
    movieSynopsisRemaining = spanMovieSynopsisRemaining.get_text()
    movieSynopsis = movieSynopsis + movieSynopsisRemaining

    contentRating = tdContentRating.get_text()
    m = re.search('(.*)\s+\((.*)\).*',contentRating)
    if m:
        try: 
            rating = m.group(1)
            metaData['rating'] = rating
        except: pass
        try:
            ratingNotes = m.group(2)
            metaData['ratingNotes'] = ratingNotes
        except: pass
    metaData['tmeter_all'] = int(allCriticsRatingValue)
    metaData['tmeter_top'] = int(topCriticsRatingValue)
    metaData['criticConsensus'] = criticConsensus
    metaData['synopsis'] = movieSynopsis
    metaData['genres'] = ",".join([span.get_text() for span in spansGenre])
    metaData['releasedate'] = tdDatePublished.get_text().strip()
    metaData['studio'] = spanProductionCompany.get_text()
    metaData['runtime'] = timeDuration['datetime']
    
    
    return 'getMovieMetaDataRT.Succes',metaData
    


    # pull director(s)
    td_director =  table_info.find("td",attrs={"itemprop":"director"})
    if(td_director):
        director = []
        spans = td_director.findAll("span",attrs={"itemprop":"name"}) 
        for span in spans:  
            director.append(span.get_text())   
    
    # pull writer(s)
    writer = []
    for row in table_info.findAll("tr"):
        if row.find("td",text=re.compile(".*Written.*")):
            alinks = row.findAll("a")
            for a in alinks:
                writer.append(a.get_text())
            break





def getMovieReviewDataRT(url):
    if('queryRTError' in url):
        return
    
    review_link = url+'reviews/'
    print review_link

    try:
        res = urllib2.urlopen(review_link)
    except (urllib2.URLError, urllib2.HTTPError):
        return 'getMovieReviewLinksRT.urlopen'

    soup = bs(res.read(),'lxml')
    pageInfo = soup.find("span",attrs={"class":"pageInfo"}).get_text()
    p = re.compile(".*Page.*(\d).*of.*(\d)")
    m = p.match(pageInfo)
    try:
        onPage   = int(m.group(1))
        numPages = int(m.group(2))
    except IndexError:
        numPages = 1
        onPage   = 1

    for page in range(numPages):
        
        div_review_rows = soup.find('div',attrs={"class":"content"}) \
            .findAll('div',attrs={"class":"review_table_row"})
                
        for div_review in div_review_rows:
            div_top_critic  = div_review.find("div",attrs={"class":"top_critic"})
            top_critic = 'Top Critic' in div_top_critic.get_text().strip()                    

            div_critic_name = div_review.find("div",attrs={"class":"critic_name"})
            if div_critic_name.find("a"): critic_name = div_critic_name.find("a").get_text()
            else: critic_name = ''
            if div_critic_name.find("em"): critic_src  = div_critic_name.find("em").get_text()
            else: critic_src = ''
                    
            div_container = div_review.find("div",attrs={"class":"review_container"})
            div_review_icon = div_container.find("div",attrs={"class":"review_icon"})
            fresh = 'fresh' in div_review_icon.attrs['class']
            review_date = div_container.find("div",attrs={"class":"review_date"}).get_text().strip()
            blurb = div_container.find("div",attrs={"class":"the_review"}).get_text().encode('ascii','ignore')
            if div_container.find("a"): review_url = div_container.find("a")['href']
            else: review_url = ''
            
            print u"{0:<40s} {1:<40s} {2:<7s} {3:<7s} {4:20s}" \
                .format(critic_name,critic_src, str(top_critic), str(fresh), review_date)
            #print "{0:} {1:}".format(review_url,blurb)

        print "Page: {0:}".format(onPage)
        onPage = onPage + 1
        new_url = review_link + '?page=' + str(onPage) + '&sort='

        try:
            res = urllib2.urlopen(new_url)
        except (urllib2.URLError, urllib2.HTTPError):
            return 'getMovieReviewLinksRT.urlopen'
        soup = bs(res.read(),'lxml')
    
    return soup

