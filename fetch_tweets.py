import twint

c = twint.Config()
c.Username = "elonmusk"  # Username of Elon Musk twitter account
c.Retweets = True  # also get retweets
c.Since = "2021-01-01"
c.Count = True  # get number of tweets fetched

c.Store_csv = True  # store as csv
c.Output = "elonmusk_tweets.csv"

twint.run.Search(c)
