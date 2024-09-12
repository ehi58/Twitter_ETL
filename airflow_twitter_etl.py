import os
import tweepy
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Load environment variables
load_dotenv()

# Database connection using environment variables
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")  # Default to localhost if not specified
DB_PORT = os.getenv("DB_PORT", "5432")  # Default PostgreSQL port if not specified
DB_NAME = os.getenv("DB_NAME")

conn_string = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
engine = create_engine(conn_string)
Session = sessionmaker(bind=engine)

# Define database models
Base = declarative_base()

class KeywordTweet(Base):
    __tablename__ = 'keyword_tweets'
    id = Column(Integer, primary_key=True)
    tweet_id = Column(String)
    text = Column(Text)
    created_at = Column(DateTime)
    author_id = Column(String)
    conversation_id = Column(String)
    searched_term = Column(String)
    filename = Column(String)

class UserTweet(Base):
    __tablename__ = 'user_tweets'
    id = Column(Integer, primary_key=True)
    username = Column(String)
    tweet_id = Column(String)
    text = Column(Text)
    created_at = Column(DateTime)
    retweets = Column(Integer)
    likes = Column(Integer)
    filename = Column(String)

# Create tables
Base.metadata.create_all(engine)

# Airflow DAG configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 26),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'twitter_etl',
    default_args=default_args,
    description='A DAG to extract tweets and user data weekly and store in PostgreSQL',
    schedule_interval='0 0 * * 0',  # Run at midnight every Sunday
    catchup=False
)

dag.doc_md = """
    This DAG extracts tweets based on specific keywords and from a list of users.
    It runs weekly on Sundays at midnight and stores the extracted data in a PostgreSQL database.
"""

# Twitter API credentials from environment variables
bearer_token = os.getenv("TWITTER_BEARER_TOKEN")
consumer_key = os.getenv("TWITTER_CONSUMER_KEY")
consumer_secret = os.getenv("TWITTER_CONSUMER_SECRET")
access_token = os.getenv("TWITTER_ACCESS_TOKEN")
access_token_secret = os.getenv("TWITTER_ACCESS_TOKEN_SECRET")

# Initialize Tweepy client
tweepy_client = tweepy.Client(
    bearer_token=bearer_token,
    consumer_key=consumer_key,
    consumer_secret=consumer_secret,
    access_token=access_token,
    access_token_secret=access_token_secret,
    wait_on_rate_limit=True
)

def get_tweets_by_keywords(keywords, max_results=10):
    tweets_data = []
    for keyword in keywords:
        try:
            tweets = tweepy_client.search_recent_tweets(
                query=keyword,
                tweet_fields=['id', 'text', 'created_at', 'author_id', 'conversation_id'],
                max_results=max_results
            )
            if tweets.data:
                for tweet in tweets.data:
                    tweets_data.append({
                        'tweet_id': tweet.id,
                        'text': tweet.text,
                        'created_at': tweet.created_at,
                        'author_id': tweet.author_id,
                        'conversation_id': tweet.conversation_id,
                        'searched_term': keyword
                    })
        except Exception as e:
            print(f"An error occurred while fetching tweets for '{keyword}': {e}")
    
    return tweets_data

def fetch_tweets(usernames, num_tweets=10):
    all_tweets = []
    for username in usernames:
        try:
            user = tweepy_client.get_user(username=username)
            user_id = user.data.id
            tweets = tweepy_client.get_users_tweets(
                id=user_id,
                max_results=num_tweets,
                tweet_fields=['id', 'text', 'created_at', 'public_metrics']
            )
            if tweets.data:
                for tweet in tweets.data:
                    all_tweets.append({
                        'username': username,
                        'tweet_id': tweet.id,
                        'text': tweet.text,
                        'created_at': tweet.created_at,
                        'retweets': tweet.public_metrics['retweet_count'],
                        'likes': tweet.public_metrics['like_count']
                    })
        except Exception as e:
            print(f"Error fetching tweets for {username}: {e}")
    
    return all_tweets

def extract_keyword_tweets():
    targeted_words = ['Broadcom', 'AVGO', 'VMware broadcom', 'VMware']
    tweets = get_tweets_by_keywords(targeted_words)
    filename = f'AVGO_tweets_{datetime.now().strftime("%m_%d_%Y-%H-%M-%S")}'
    
    session = Session()
    for tweet in tweets:
        new_tweet = KeywordTweet(
            tweet_id=tweet['tweet_id'],
            text=tweet['text'],
            created_at=tweet['created_at'],
            author_id=tweet['author_id'],
            conversation_id=tweet['conversation_id'],
            searched_term=tweet['searched_term'],
            filename=filename
        )
        session.add(new_tweet)
    session.commit()
    session.close()

def extract_user_tweets():
    users = ['NickTimiraos', 'BurryArchive', 'michaeljburry', 'nvidia', 'nvidiaomniverse', 'NVIDIAAIDev', 'NVIDIADC', 'realMeetKevin', 'unusual_whales', 'RayDalio', 'stlouisfed']
    tweets = fetch_tweets(users, num_tweets=10)
    filename = f'user_tweets_{datetime.now().strftime("%m_%d_%Y-%H-%M-%S")}'
    
    session = Session()
    for tweet in tweets:
        new_tweet = UserTweet(
            username=tweet['username'],
            tweet_id=tweet['tweet_id'],
            text=tweet['text'],
            created_at=tweet['created_at'],
            retweets=tweet['retweets'],
            likes=tweet['likes'],
            filename=filename
        )
        session.add(new_tweet)
    session.commit()
    session.close()

# Define tasks
keyword_tweets_task = PythonOperator(
    task_id='extract_keyword_tweets',
    python_callable=extract_keyword_tweets,
    dag=dag,
)

user_tweets_task = PythonOperator(
    task_id='extract_user_tweets',
    python_callable=extract_user_tweets,
    dag=dag,
)

# Set task dependencies
keyword_tweets_task >> user_tweets_task