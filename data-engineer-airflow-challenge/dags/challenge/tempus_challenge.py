import datetime
import requests
import json
import logging
import boto3
import pandas as pd
from airflow.models import Variable

NEWS_API_KEY = Variable.get('NEWS_API_KEY')
AWS_ACCESS_KEY_ID = Variable.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = Variable.get('AWS_SECRET_ACCESS_KEY')
S3_BUCKET = Variable.get('S3_BUCKET')



class NewsAPI:
    """
    Class for pulling data from the News API
    """

    def __init__(self):
        self.api_base_url = 'https://newsapi.org/v2/'
        self.source_url = self.api_base_url + 'sources'
        self.headlines_url = self.api_base_url + 'top-headlines'
        self.key_params = {'apiKey': NEWS_API_KEY}

    def get_query_params(self, additional_params) -> dict:
        """
        Query parameter builder pattern. Always uses the apiKey as base
        :param additional_params:
        :return: all parameters in on dictionary
        """
        query_params = dict()
        query_params.update(self.key_params)
        if additional_params:
            query_params.update(additional_params)
        return query_params

    def json_to_dict(self, json_obj) -> dict:
        """
        Attempt to convert JSON into a dictionary
        :param json_obj: JSON object
        :return: dictionary representation of JSON object
        """
        try:
            return json.loads(json_obj)
        except ValueError as e:
            logging.error(f'Failed due to Malformed JSON in response from request {json_obj}')
            logging.error(e)
            exit(3)

    def get_source_ids(self, additional_params: dict) -> list:
        """
        Pull from the /sources API based on any valid additional parameters
        :param additional_params:
        :return: list of source's ids
        """
        response = requests.get(self.source_url, params=self.get_query_params(additional_params))

        if response.ok:
            content = self.json_to_dict(response.content)
            return [source.get('id') for source in content.get('sources', [])]
        else:
            logging.error(f'Bad response for request {self.source_url}{additional_params}')
            logging.error(f'Response code: {response}')
            raise ConnectionError('Bad Response from sources')


    def get_headlines(self, source_id: str):
        """
        Pull from the /top-headlines API based on a given source_id and flatten the results
        :param source_id:
        :return:
        """
        response = requests.get(self.headlines_url, params=self.get_query_params({"sources": source_id}))

        if response.ok:
            response_content = json.loads(response.content)
            headlines = []
            # Flatten each article headline
            for article in response_content.get('articles', []):
                headlines.append(flatten_json(article))
            return headlines
        else:
            logging.error(f'Bad response for request {self.api_base_url}{source_id}')
            logging.error(f'Response code: {response}')
            logging.error(f'Response content: {response.content}')
            raise ConnectionError('Bad Response from headlines')

    def save_to_s3(self, content: list, execution_date: datetime.datetime):
        """
        Convert to data frame for easy cleanup, then save to s3
        :param content: list of dictionaries, one dictionary per row
        :param execution_date: Airflow's "ds"
        """
        df = pd.DataFrame(content)

        # Remove common funky characters
        df.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=["", ""], regex=True, inplace=True)
        string_to_s3(content=df.to_csv(index=False), s3_location=f'{execution_date}_top_headlines.csv')


def get_sources(**kwargs) -> list:
    client = NewsAPI()
    context = kwargs['params']
    return client.get_source_ids(additional_params=context)


def save_headlines(**kwargs):
    client = NewsAPI()
    airflow_ds = kwargs['ds']
    sources = kwargs['task_instance'].xcom_pull(task_ids='get_en_news_sources')

    if not sources:
        raise ValueError('Integration between tasks failed from get_sources() to save_headlines().')

    headlines = []
    for source in sources:
        for headline in client.get_headlines(source_id=source):
            headlines.append(headline)
    client.save_to_s3(content=headlines, execution_date=airflow_ds)


def string_to_s3(content: str, s3_location: str):
    """
    Simple helper function used to move a string into an S3 object
    :param content: string of the output you want
    :param s3_location: Path in S3
    """
    s3 = boto3.client('s3',
                      aws_access_key_id=AWS_ACCESS_KEY_ID,
                      aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    try:
        s3.put_object(Body=content,
                      Bucket=S3_BUCKET,
                      Key=s3_location)

        # Confirm file exists
        s3.head_object(Bucket=S3_BUCKET,
                       Key=s3_location)

    except Exception as e:
        raise ValueError("Unable to upload to S3 during string_to_s3()")


def flatten_json(json_data) -> dict or list:
    """
    Helper function for flatten JSON. Works on the headline or any other JSON object
    :param json_data: well formatted JSON
    :return: json_data (param) as a one dimensional dictionary
    """
    out = {}

    def flatten(current_obj, name=''):
        if type(current_obj) is dict:
            for key in current_obj:
                flatten(current_obj[key], name + key + '.')
        elif type(current_obj) is list:
            i = 0
            for field in current_obj:
                flatten(field, name + str(i) + '.')
                i += 1
        else:
            out[name[:-1]] = current_obj
    flatten(json_data)
    return out
