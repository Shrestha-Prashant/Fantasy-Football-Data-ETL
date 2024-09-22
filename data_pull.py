import requests, json
from pprint import pprint
import pandas as pd

# base url
base_url = 'https://fantasy.premierleague.com/api/'

# bootstrap-static element
r = requests.get(base_url+'bootstrap-static/').json()

# representign top level fields
# pprint(r, indent=2, depth=1, compact=True)

#getting player data from 'elements' field
players = r['elements']

# showing data for first player
# pprint(players[0])

pd.set_option('display.max_columns', None)

# creating player dataframe
players = pd.json_normalize(r['elements'])
# print(players)


# showing some information about first five players
# print(players[['id', 'web_name', 'team', 'element_type']].head())
print(players[['id','web_name','chance_of_playing_this_round','chance_of_playing_next_round','selected_by_percent','total_points','points_per_game_rank']].head())