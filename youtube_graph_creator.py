# -*- coding: utf-8 -*-

# Sample Python code for youtube.channels.list
# See instructions for running these code samples locally:
# https://developers.google.com/explorer-help/guides/code_samples#python

import os
import sys
import csv
import copy
import googleapiclient.discovery
from collections import defaultdict
from tools.logger import logger
import datetime
from dateutil import parser
import pytz

global today 
global five_years_ago
today = pytz.utc.localize(datetime.datetime.utcnow())
logger.debug("%s; Today: %s", ['Youtube_Graph_Creator', 'global'], today)
five_years_ago = today - datetime.timedelta(days=5*365)

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
                    'depth': 0,
                    'class': None,
                    'targeted': 0
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

    def _clean_edges(self):
        bad_edges = []
        for x in range(len(self.edges)):
            if self.edges[x][1] not in self.nodes.keys():
                bad_edges.append(x) #target node didn't return successful api request (Likely explanation)
        self.edges = [v for i,v in enumerate(self.edges) if i not in frozenset(bad_edges)] 
        for x in range(len(self.edges)):
            for i in range(len(self.edges[x])):
                for node in self.nodes.keys():
                    try:
                        if self.nodes[node]['id'] == self.edges[x][i]:
                            self.edges[x][i]=self.nodes[node]['title']
                    except Exception as e:
                        logger.debug("%s; Node: %s failed with exception %s. self.nodes[node]: %s", ['Youtube_Graph_Creator', '_clean_edges'], node, e, self.nodes[node])

    def save_graph(self, out_file, checkpoint=False):
        
        logger.debug("%s; Number of nodes in graph: %s", ['Youtube_Graph_Creator', 'save_graph'], self._get_node_graph_size())
        logger.debug("%s; Number of edges in graph: %s", ['Youtube_Graph_Creator', 'save_graph'], self._get_node_edge_count())
        
        attributes = set()
        for k,v in self.nodes.items():
            attributes.update(v.keys())
        
        with open(out_file + '.csv','w',newline='') as f:
            w = csv.DictWriter(f,fieldnames=['Channel Title']+list(sorted(attributes)))
            w.writeheader()
            for k,v in self.nodes.items():
                temp = dict(v)
                temp['Channel Title'] = k
                w.writerow(temp)
        logger.debug("%s; Saved node graph", ['Youtube_Graph_Creator', 'save_graph'])

        if not checkpoint:
            self._clean_edges()

        with open(out_file + '_edges.csv', "w") as f:
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
        logger.debug("%s; 'items' in response: %s", ['Youtube_Graph_Creator', 'explore_from_node'], 'items' in response.keys())
        return response

    def _determine_class(self, node_data):
        node_class = 4
        try:
            if node_data['video count'] > 150 and node_data['channel age'] < five_years_ago:
                if node_data['subscribers'] > 1000000:
                    node_class = 1
                else: 
                    node_data = 2
        except Exception as e:
            logger.debug("%s; comparison failed with Exception: %s", ['Youtube_Graph_Creator', '_determine_class'], e)

        return node_class
    def _parse_node_data(self, node_data):
        parsed_data = {}
        try:
            parsed_data['id'] = node_data['items'][0]['id']
        except Exception as e:
            logger.debug("%s; Exception raised: %s. Basically this account doesn't exist anymore", ['Youtube_Graph_Creator', '_parse_node_data'], e)
        parsed_data['title'] = node_data['items'][0]['snippet']['title']
        parsed_data['channel age'] = parser.parse(node_data['items'][0]['snippet']['publishedAt'])
        parsed_data['subscribers'] = int(node_data['items'][0]['statistics']['subscriberCount'])
        parsed_data['video count'] = int(node_data['items'][0]['statistics']['videoCount'])
        parsed_data['view count'] = int(node_data['items'][0]['statistics']['viewCount'])
        try:
            parsed_data['topics'] = node_data['items'][0]['topicDetails']['topicCategories']
        except Exception as e:
            parsed_data['topics'] = None
            logger.debug("%s; Exception raised: %s", ['Youtube_Graph_Creator', '_parse_node_data'], e)
        parsed_data['description'] = node_data['items'][0]['snippet']['description']
        node_class = self._determine_class(parsed_data)
        parsed_data['class'] = node_class
        return parsed_data

    def explore_from_node(self, node, username=False):
        depth = self._get_node_depth(node)
        neighbors = []
        if  depth == 0:
            username=True
        logger.debug("%s; Arguments passed to request: %s", ['Youtube_Graph_Creator', 'explore_from_node'], (node, username))
        node_data = self._query_youtube_channel(node, username)
        if 'items' in node_data.keys():
            parsed_node_data = self._parse_node_data(node_data)
            self.nodes[node] = {**self.nodes[node], **parsed_node_data}
            self.nodes[node]['found'] = True
            node_id = self.nodes[node]['id']
            try:
                neighbors = node_data['items'][0]['brandingSettings']['channel']['featuredChannelsUrls']
            except:
                logger.debug("%s; Node: %s has no featured channels", ['Youtube_Graph_Creator', 'explore_from_node'], node)
            neighbor_depth = (depth + 1)
            for neighbor in neighbors:
                self.edges.append([node_id, neighbor])
                if neighbor not in self.nodes.keys():
                    self.nodes[neighbor] = {'depth': neighbor_depth, 'seed group': self._get_parent_seed_group(node), 'targeted': 1}
                else:
                    self.nodes[neighbor]['targeted'] = (self.nodes[neighbor]['targeted'] + 1)
        else: 
            logger.debug("%s; query on %s missing data", ['Youtube_Graph_Creator', 'explore_from_node'], node)
            del self.nodes[node]
        logger.debug("%s; Number of nodes in graph: %s", ['Youtube_Graph_Creator', 'explore_from_node'], self._get_node_graph_size())
        logger.debug("%s; Number of edges in graph: %s", ['Youtube_Graph_Creator', 'explore_from_node'], self._get_node_edge_count())
        return neighbors

    def get_node_information(self, node):
        try:
            node_data = self._query_youtube_channel(node)
            parsed_node_data = self._parse_node_data(node_data)
            self.nodes[node] = {**self.nodes[node], **parsed_node_data}
            self.nodes[node]['found'] = True
        except Exception as e:
            logger.debug("%s; query on %s raised exception: %s", ['Youtube_Graph_Creator', 'get_node_information'], node, e)
            del self.nodes[node]

    def _check_connector_status(self):
        for node in self.nodes.keys():
            if self.nodes[node]['targeted'] > 3 and self.nodes[node]['class'] > 2:
                self.nodes[node]['class'] = 3

    def explore_seed_graph(self):
        visited = set()
        node_stack = []
        new_list = list(range(10))
        print_levels = [ 150 * x for x in new_list]
        print_level = 0
        for seed in self.seeds:
            node_stack.append(seed)
        while len(node_stack):
            if len(self.nodes.keys()) > print_levels[print_level]:
                self.save_graph('./output/checkpoint.csv', checkpoint=True)
                print_level += 1
            node = node_stack.pop()
            depth = self._get_node_depth(node)
            if node not in visited:
                visited.add(node)
                if depth < 3:
                    neighbors = self.explore_from_node(node)
                    node_stack += neighbors
                else:
                    self.get_node_information(node)
            else:
                continue
        self._check_connector_status()
