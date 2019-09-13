'''Module main'''
import json
from file import File

class App:
    '''class Application'''

    @classmethod
    def __init__(cls):
        '''Method init'''
        cls.config = json.loads(open('config/config.json').read())

    @classmethod
    def main(cls):
        '''start application'''
        File.main(cls.config['source'])

if __name__ == '__main__':
    App().main()
