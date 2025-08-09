""" Fetches relevent Reddit posts & comments and returns a Pandas DataFrame of them"""

import os
import pandas as pd
import praw
from dotenv import load_dotenv

# Load our keys from .env file
load_dotenv()

def fetch_reddit_sentiment(subreddit: str, keywords: list[str], limit: int = 50) -> pd.DataFrame:
    """
    - Connects to Reddit via PRAW
    - Pull top `limit` posts from r/{NBA}.
    - Filter posts whose title or body mention any of `keywords`.
    - For each, extract: post_id, created_utc, score, num_comments, title, selftext.
    - Return a DataFrame.
    """

    # Initialize PRAW reddit object with our credentials
    reddit = praw.Reddit(
        client_id=os.environ.get('REDDIT_CLIENT_ID'),
        client_secret=os.environ.get('REDDIT_CLIENT_SECRET'),
        user_agent=os.environ.get('REDDIT_USER_AGENT')
    )

    # Get the PRAW subreddit object with our desired subreddit, in this case r/NBA
    subreddit = reddit.subreddit(subreddit)

    # Create a temp array to hold all our submission data
    all_rows_data = []

    # Collect top 50 comments of posts that are of game threads within the last month
    # If post has less than 50 comments, just take all comments

    # Get posts that contain 'Game Thread' in the title, sorted by num of comments, within the last month
    for submission in subreddit.search(keywords, sort="comments", time_filter="month"):
        submission_data = {'gameID': submission.id,
                           'title': submission.title,
                           'numComments': submission.num_comments,
                           'top50Comments': [comment.body for comment in submission.comments[:limit]]}
        all_rows_data.append(submission_data)

    # Create DF with our submission data
    submission_df = pd.DataFrame(all_rows_data)

    return submission_df


if __name__ == '__main__':
    print(fetch_reddit_sentiment('nba', 'Game Thread').head())
