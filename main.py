from youtube_graph_creator import YoutubeGraphCreator

graph_creator = YoutubeGraphCreator()
file_name = './data/seed_nodes.csv'
graph_creator.load_seed_graph(file_name)
graph_creator.explore_seed_graph()
graph_creator.save_graph()