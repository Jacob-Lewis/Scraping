# -*- coding: utf-8 -*-

# Sample Python code for youtube.channels.list
# See instructions for running these code samples locally:
# https://developers.google.com/explorer-help/guides/code_samples#python

import os
import csv
import googleapiclient.discovery

class YoutubeGraphCreator(object):

    def __init__(self, max_depth=3):
        self.max_depth=max_depth
        self.nodes = {}
        self.edges = {}

    def load_seed_graph(self, seed_file):
        """
        seed_file is a csv file. The first column in the file is the title of a youtube channel
        """
        with open(seed_file, newline='') as csvfile:
            spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')
            for row in spamreader:
                self.nodes[row[0]] = {
                    'seed group': None,
                    'topics': None,
                    'subscribers': None,
                    'channel age': None, 
                    'video count': None,
                    'view count': None
                }

    

def query_youtube_channel(identifier, username=False):

    # Disable OAuthlib's HTTPS verification when running locally.
    # *DO NOT* leave this option enabled in production.
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

    api_service_name = "youtube"
    api_version = "v3"
    DEVELOPER_KEY = "AIzaSyCX_mf5QX25BSSv7HRsPKYrI3dvboid60E"

    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey = DEVELOPER_KEY)

    #id="UC_x5XG1OV2P6uZZ5FSM9Ttw",
    #forUsername="tibees",
    if username:
        request = youtube.channels().list(
            forUsername=identifier,
            part="brandingSettings,contentOwnerDetails,snippet,contentDetails,statistics,topicDetails,status"
        )
    else:
        request = youtube.channels().list(
            id=identifier,
            part="brandingSettings,contentOwnerDetails,snippet,contentDetails,statistics,topicDetails,status"
        )
    response = request.execute()
    return response
