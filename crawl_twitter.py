import sys
import time
from urllib2 import URLError
from httplib import BadStatusLine
import twitter
import json
from collections import Counter
import re
import pickle

def oauth_login():
    # Fill up these secrets yourself
    CONSUMER_KEY = ''
    CONSUMER_SECRET = ''
    OAUTH_TOKEN = ''
    OAUTH_TOKEN_SECRET = ''

    auth = twitter.oauth.OAuth(OAUTH_TOKEN, OAUTH_TOKEN_SECRET, CONSUMER_KEY, CONSUMER_SECRET)

    twitter_api = twitter.Twitter(auth=auth)

    return twitter_api

def get_rt_attributions(tweet):

    # Regex adapted from Stack Overflow (http://bit.ly/1821y0J)

    rt_patterns = re.compile(r"(RT|via)((?:\b\W*@\w+)+)", re.IGNORECASE)
    rt_attributions = []

    # Inspect the tweet to see if it was produced with /statuses/retweet/:id.
    # See https://dev.twitter.com/docs/api/1.1/get/statuses/retweets/%3Aid.
    
##    if tweet.has_key('retweeted_status'):
##        attribution = tweet['retweeted_status']['user']['screen_name'].lower()
##        rt_attributions.append(attribution)

    # Also, inspect the tweet for the presence of "legacy" retweet patterns
    # such as "RT" and "via", which are still widely used for various reasons
    # and potentially very useful. See https://dev.twitter.com/discussions/2847 
    # and https://dev.twitter.com/discussions/1748 for some details on how/why.

    try:
        rt_attributions += [ 
                        mention.strip() 
                        for mention in rt_patterns.findall(tweet['text'])[0][1].split() 
                      ]
    except IndexError, e:
        pass

    # Decide the affecters and affectees of the tweet
    if rt_attributions:
        # it's a retweet
        affecters = [user_mention['id_str']
                        for rta in rt_attributions
                        for user_mention in tweet['entities']['user_mentions']
                        if user_mention['screen_name'] == rta.strip('@')]
        affectees = [tweet['user']['id_str']]
    else:
        # it's a mention tweet or normal tweet
        if 'user_mentions' in tweet['entities']:
            # it's a mention tweet
            affecters = [tweet['user']['id_str']]
            affectees = [user_mention['id_str'] for user_mention in tweet['entities']['user_mentions']]
        else:
            # it's a normal tweet without mention
            affecters = []
            affectees = []
    return affecters, affectees

def make_twitter_request(twitter_api_func, max_errors=10, *args, **kw): 
    
    # A nested helper function that handles common HTTPErrors. Return an updated
    # value for wait_period if the problem is a 500 level error. Block until the
    # rate limit is reset if it's a rate limiting issue (429 error). Returns None
    # for 401 and 404 errors, which requires special handling by the caller.
    def handle_twitter_http_error(e, wait_period=2, sleep_when_rate_limited=True):
    
        if wait_period > 3600: # Seconds
            print >> sys.stderr, 'Too many retries. Quitting.'
            raise e
    
        # See https://dev.twitter.com/docs/error-codes-responses for common codes
    
        if e.e.code == 401:
            print >> sys.stderr, 'Encountered 401 Error (Not Authorized)'
            return None
        elif e.e.code == 404:
            print >> sys.stderr, 'Encountered 404 Error (Not Found)'
            return None
        elif e.e.code == 429: 
            print >> sys.stderr, 'Encountered 429 Error (Rate Limit Exceeded)'
            if sleep_when_rate_limited:
                print >> sys.stderr, "Retrying in 15 minutes...ZzZ..."
                sys.stderr.flush()
                time.sleep(60*15 + 5)
                print >> sys.stderr, '...ZzZ...Awake now and trying again.'
                return 2
            else:
                raise e # Caller must handle the rate limiting issue
        elif e.e.code in (500, 502, 503, 504):
            print >> sys.stderr, 'Encountered %i Error. Retrying in %i seconds' % \
                (e.e.code, wait_period)
            time.sleep(wait_period)
            wait_period *= 1.5
            return wait_period
        else:
            raise e

    # End of nested helper function
    
    wait_period = 2 
    error_count = 0 

    while True:
        try:
            return twitter_api_func(*args, **kw)
        except twitter.api.TwitterHTTPError, e:
            error_count = 0 
            wait_period = handle_twitter_http_error(e, wait_period)
            if wait_period is None:
                return
        except URLError, e:
            error_count += 1
            print >> sys.stderr, "URLError encountered. Continuing."
            if error_count > max_errors:
                print >> sys.stderr, "Too many consecutive errors...bailing out."
                raise
        except BadStatusLine, e:
            error_count += 1
            print >> sys.stderr, "BadStatusLine encountered. Continuing."
            if error_count > max_errors:
                print >> sys.stderr, "Too many consecutive errors...bailing out."
                raise

def extract_tweet_entities(stream, num=1000000):
    count = 0
    data = []
    for tweet in stream:
        affecters, affectees = get_rt_attributions(tweet)
        if affecters and affectees:
            data.append([affecters, affectees, tweet['text']])
            count += 1
            # print count
            if count > num:
                break
    with open('twitter_preprocessed', 'w') as f:
         pickle.dump(data, f)
    return data

def crawl_with_stream(q=None, num=1000000):
    twitter_api = oauth_login()
    twitter_stream = twitter.TwitterStream(domain='stream.twitter.com', auth=twitter_api.auth)
    if q:
        stream = twitter_stream.statuses.filter(track=q, lang='en')
    else:
        stream = twitter_stream.statuses.sample()
    count = 0
    data = []
    for tweet in stream:
        affecters, affectees = get_rt_attributions(tweet)
        if affecters and affectees:
            data.append([affecters, affectees, tweet['text']])
            count += 1
            print count
            if count > num:
                break
    with open('twitter_preprocessed', 'w') as f:
         pickle.dump(data, f)
    return data
    # return extract_tweet_entities(stream, num=1000000)

def crawl_with_rest(q='iphone', num=1000000):
    twitter_api = oauth_login()
    count = 100
    search_results = twitter_api.search.tweets(q=q, count=count, lang='en')
    statuses = search_results['statuses']

    for _ in range(0, num/count+1):
        print "Length of statuses", len(statuses)
        print "search_metadata", search_results['search_metadata']
        try:
            next_results = search_results['search_metadata']['next_results']
            # print "Next results", next_results
        except KeyError, e:
            break

        search_results = twitter_api.search.tweets(q=q, max_id=long(search_results['search_metadata']['next_results'][1:].split('&')[0].split('=')[1]),
                                                   count = count, include_entities=1)
        statuses += search_results['statuses']
    with open('twitter_raw', 'w') as f:
         pickle.dump(statuses, f)
    return extract_tweet_entities(statuses, num)
