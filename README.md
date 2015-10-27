# WebScraping

#### RottenTomatoes (RT)

The following functions are defined:

**QueryRT**([array]movie) - Performs a query on a movie title and movie year returning URL to best match RT movie web page. Failing that it returns an error code.  Input is an [array]movie = ([string]year,[string]title).
 
**getMovieMetaData**([string]url) - Given a RT movie page URL, return a dictionary of movie meta data including:
 - date movie was published
 - movie runtime
 - tomato meter for all critics
 - tomato meter for top critics
 - text blurb for critic consensus
 - list of writer(s)
 - list of director(s)
 - list of actors and characters
 - name of production company
 - list of movie genre(s)
 - movie rating
 - movie rating description text


**getMovieReviewLinksRT**([string]url) - returns critic review information including:
 - critic name
 - publication source
 - review text blurb
 - fresh or rotten [boolean]
 - top cricit flag [boolean]
 - URL to full review
