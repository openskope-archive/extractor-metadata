#!/usr/bin/env python

"""Example extractor based on the clowder code.""" 
import logging 
import subprocess
import json
import random
import subprocess
import stat


from elasticsearch import Elasticsearch
from pyclowder.extractors import Extractor
import pyclowder.datasets 
import pyclowder.files
import pyclowder.clowder 
import os


class Metadata(Extractor):
    '''
    An extractor used to abstract newly added model run 
    metadata information from the dataset and pushed it to 
    the elastic search engine
    '''
    def __init__(self):
        Extractor.__init__(self)

        # add any additional arguments to parser
        # self.parser.add_argument('--max', '-m', type=int, nargs='?', default=-1,
        #                          help='maximum number (default=-1)')

        # parse command line and load default logging configuration
        self.setup()

        # setup logging for the exctractor
        logging.getLogger('pyclowder').setLevel(logging.DEBUG)
        logging.getLogger('__main__').setLevel(logging.DEBUG)

        #Using the clowder module to directly interact with clowder
        self.clowder = pyclowder.clowder.Clowder()
        
        
    def check_message(self, connector, host, secret_key, resource, parameters):
        #TODO: Later can be used to check the integrity of the metadata info
        return CheckMessage.bypass

    
    def process_message(self, connector, host, secret_key, resource, parameters):
        logger = logging.getLogger(__name__)
        #TODO: Make sure that all the metadata info are relavant only to the dataset

        #TODO: Check that the dataset info that we get is actualy related 
        #TODO: to the newly added metadata

        #Get dataset info and all the newly added metadata info
        ds_info = pyclowder.datasets.get_info(connector, host, secret_key, resource['parent']['id'])
        index_id = resource['parent']['id']
        logger.debug("The relevant dataset name is "+ ds_info['name'])
        newly_added_metadata = resource['metadata']
        
        #TODO: Check that the newly_added_metadata info actully has
        #TODO: All the info

        #TODO: Connect through container
        #TODO: Do we have a unified doc type?
        es = Elasticsearch()
        es.index(index=ds_info['name'], doc_type='dataset_meta', id=index_id, 
                body=json.loads(resource['metadata']))
        logger.debug("Finished uploading the metadata to the elastic serach")
       

if __name__ == "__main__":
    extractor = Paleo()
    extractor.start()
