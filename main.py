from youtube_graph_creator import YoutubeGraphCreator
from tools.logger import logger
import datetime

todays_print = datetime.datetime.today().strftime('%m-%d')
logger.debug("%s; today's print: %s", ['Youtube_Graph_Creator', 'global'], todays_print)
in_file = './data/seed_nodes_v2.csv'
out_file = './output/nodes_' + todays_print

graph_creator = YoutubeGraphCreator()

graph_creator.load_seed_graph(in_file)
graph_creator.explore_seed_graph()
graph_creator.save_graph(out_file)