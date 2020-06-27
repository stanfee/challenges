import datetime
import pytest
from dags import challenge as c


class TestFlattening:
    def test_flatten1(self):
        dictionary1 = {
            'ds': datetime.datetime.now().isoformat().split('T')[0],
            'params': {
                'name': 'World',
            },
        }
        expected = {'ds': datetime.datetime.now().isoformat().split('T')[0],
                    'params.name': 'World'}
        assert expected == c.flatten_json(dictionary1)

    def test_flatten2(self):
        dictionary2 = {
            'source': {'id': 'espn-cric-info',
                       'name': 'ESPN Cric Info'},
            'author': None,
            'title': "I'm fighting my own benchmarks - R Ashwin | ESPNcricinfo.com", 'description': "India's No. 1 offspinner talks to Manjrekar on his form abroad, injuries and more | ESPNcricinfo.com",
            'url': 'http://www.espncricinfo.com/story/_/id/29102228/fighting-my-own-benchmarks-r-ashwin',
            'urlToImage': 'https://a4.espncdn.com/combiner/i?img=%2Fi%2Fcricket%2Fcricinfo%2F1219773_1296x729.jpg',
            'publishedAt': '2020-04-25T03:00:09Z',
            'content': 'R Ashwin has said that he is "fighting my own benchmarks" because his Test performances overseas are being measured against his heroics in India. Despite being the country\'s best long-form spinner in… [+3347 chars]'
           }
        expected = {'source.id': 'espn-cric-info', 'source.name': 'ESPN Cric Info', 'author': None, 'title': "I'm fighting my own benchmarks - R Ashwin | ESPNcricinfo.com", 'description': "India's No. 1 offspinner talks to Manjrekar on his form abroad, injuries and more | ESPNcricinfo.com", 'url': 'http://www.espncricinfo.com/story/_/id/29102228/fighting-my-own-benchmarks-r-ashwin', 'urlToImage': 'https://a4.espncdn.com/combiner/i?img=%2Fi%2Fcricket%2Fcricinfo%2F1219773_1296x729.jpg', 'publishedAt': '2020-04-25T03:00:09Z', 'content': 'R Ashwin has said that he is "fighting my own benchmarks" because his Test performances overseas are being measured against his heroics in India. Despite being the country\'s best long-form spinner in… [+3347 chars]'}
        assert expected == c.flatten_json(dictionary2)

    def test_flatten3(self):
        dictionary3 = {
            'step1a':
                {'step2': ['list0', 'list1']},
            'step1b': 'string1'
        }
        expected = {'step1a.step2.0': 'list0', 'step1a.step2.1': 'list1', 'step1b': 'string1'}
        print(expected)
        print(c.flatten_json(dictionary3))
        assert expected == c.flatten_json(dictionary3)
