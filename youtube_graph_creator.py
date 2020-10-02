# -*- coding: utf-8 -*-

# Sample Python code for youtube.channels.list
# See instructions for running these code samples locally:
# https://developers.google.com/explorer-help/guides/code_samples#python

import os
import csv
import copy
import googleapiclient.discovery
from collections import defaultdict
from tools.logger import logger

class YoutubeGraphCreator(object):

    def __init__(self, max_depth=3):
        self.max_depth=max_depth
        self.seeds = {}
        self.edges = []

    def load_seed_graph(self, seed_file):
        """
        seed_file is a csv file. The first column in the file is the title of a youtube channel
        """
        with open(seed_file, newline='') as csvfile:
            spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')
            for row in spamreader:
                seed = row[0]
                self.seeds[seed] = {
                    'seed group': seed,
                    'topics': None,
                    'subscribers': None,
                    'channel age': None, 
                    'video count': None,
                    'view count': None,
                    'id': None,
                    'description': None,
                    'found': False,
                    'title': seed,
                    'depth': 0
                }
        self.nodes = self.seeds
        logger.debug("%s; %s", ['Youtube_Graph_Creator', 'load_seed_graph'], f"Number of seed nodes: {len(self.nodes.keys())}")

    def _get_node_depth(self, node):
        return self.nodes[node]['depth']

    def _get_parent_seed_group(self, node):
        return self.nodes[node]['seed group']

    def _get_node_graph_size(self):
        return len(self.nodes.keys())

    def _get_node_edge_count(self):
        return len(self.edges)

    def save_graph(self):
        
        logger.debug("%s; Number of nodes in graph: %s", ['Youtube_Graph_Creator', 'save_graph'], self._get_node_graph_size())
        logger.debug("%s; Number of edges in graph: %s", ['Youtube_Graph_Creator', 'save_graph'], self._get_node_edge_count())
        
        attributes = set()
        for k,v in self.nodes.items():
            attributes.update(v.keys())
        
        with open('./output/nodes.csv','w',newline='') as f:
            w = csv.DictWriter(f,fieldnames=['Channel Title']+list(sorted(attributes)))
            w.writeheader()
            for k,v in self.nodes.items():
                temp = dict(v)
                temp['Channel Title'] = k
                w.writerow(temp)

        with open("./output/edges.csv", "w") as f:
            writer = csv.writer(f)
            writer.writerow(["Source", "Target"])
            writer.writerows(self.edges)

    def _query_youtube_channel(self, identifier, username=False):

        # Disable OAuthlib's HTTPS verification when running locally.
        # *DO NOT* leave this option enabled in production.
        os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

        api_service_name = "youtube"
        api_version = "v3"
        DEVELOPER_KEY = "AIzaSyCX_mf5QX25BSSv7HRsPKYrI3dvboid60E"

        youtube = googleapiclient.discovery.build(
            api_service_name, api_version, developerKey = DEVELOPER_KEY)

        identifier = identifier.replace(" ", "")
        logger.debug("%s; identifier: %s", ['Youtube_Graph_Creator', 'explore_from_node'], identifier)

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
        logger.debug("%s; response: %s", ['Youtube_Graph_Creator', 'explore_from_node'], response)
        return response

    def _parse_node_data(self, node_data):
        parsed_data = {}
        parsed_data['id'] = node_data['items'][0]['id']
        parsed_data['title'] = node_data['items'][0]['snippet']['title']
        parsed_data['channel age'] = node_data['items'][0]['snippet']['publishedAt']
        parsed_data['subscribers'] = node_data['items'][0]['statistics']['subscriberCount'] 
        parsed_data['video count'] = node_data['items'][0]['statistics']['videoCount'] 
        parsed_data['view count'] = node_data['items'][0]['statistics']['viewCount']
        parsed_data['topics'] = node_data['items'][0]['topicDetails']['topicCategories']
        parsed_data['description'] = node_data['items'][0]['snippet']['description']
        return parsed_data

    def explore_from_node(self, node, username=False):
        depth = self._get_node_depth(node)
        if  depth == 0:
            username=True
        logger.debug("%s; Arguments passes to request: %s", ['Youtube_Graph_Creator', 'explore_from_node'], (node, username))
        try:
            node_data = self._query_youtube_channel(node, username)
            parsed_node_data = self._parse_node_data(node_data)
            self.nodes[node] = {**self.nodes[node], **parsed_node_data}
            self.nodes[node]['found'] = True
            node_id = self.nodes[node]['id']
            neighbors = node_data['items'][0]['brandingSettings']['channel']['featuredChannelsUrls']
            neighbor_depth = (depth + 1)
            for neighbor in neighbors:
                self.nodes[neighbor] = {'depth': neighbor_depth, 'seed group': self._get_parent_seed_group(node)}
                self.edges.append([node_id, neighbor])
        except:
            neighbors = []
        logger.debug("%s; Number of nodes in graph: %s", ['Youtube_Graph_Creator', 'explore_from_node'], self._get_node_graph_size())
        logger.debug("%s; Number of edges in graph: %s", ['Youtube_Graph_Creator', 'explore_from_node'], self._get_node_edge_count())
        return neighbors

    def explore_seed_graph(self):
        visited = set()
        node_stack = []
        for seed in self.seeds:
            node_stack.append(seed)
        while len(node_stack):
            if len(self.nodes.keys()) % 250 == 0:
                self.save_graph()
            node = node_stack.pop()
            depth = self._get_node_depth(node)
            if node not in visited and depth < 3:
                visited.add(node)
                neighbors = self.explore_from_node(node)
                node_stack += neighbors
            else:
                continue
