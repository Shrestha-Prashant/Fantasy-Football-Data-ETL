import os
import pandas as pd
import unicodedata
import random
from collections import Counter
from tabulate import tabulate

def normalize_name(name):
    """Normalize the name to remove non-ASCII characters."""
    return unicodedata.normalize('NFKD', name).encode('ASCII', 'ignore').decode('ASCII')

def ingest_player_data(players_dir):
    """Ingest player game week data from CSV files."""
    player_data = {}
    for player_folder in os.scandir(players_dir):
        if player_folder.is_dir():
            player_name, player_id = player_folder.name.rsplit('_', 1)
            csv_file_path = os.path.join(player_folder, 'gw.csv')
            symbol_name = f"{normalize_name(player_name)}_{player_id}"
            player_data[symbol_name] = pd.read_csv(csv_file_path)
    return player_data

def merge_player_data(player_data, raw_stats_df):
    """Merge raw player stats with game week data."""
    df = pd.DataFrame()
    for _, row in raw_stats_df.iterrows():
        player_id = row['id']
        player_name = f"{row['first_name']}_{row['second_name']}_{player_id}"
        symbol_name = normalize_name(player_name)
        player_gw_data = player_data.get(symbol_name)
        
        if player_gw_data is not None:
            player_gw_data.reset_index(inplace=True)
            player_gw_data['id'] = player_id
            for col in row.index:
            #     player_gw_data[col] = row[col]
                if col not in player_gw_data.columns:  
                    player_gw_data[col] = row[col]
            df = pd.concat([df, player_gw_data], ignore_index=True)
 
    df = df[['element_type', 'team', 'second_name', 'first_name', 'id', 'ict_index','total_points', 'value', 'index']]
    df = df.rename(columns={'index': 'Game_Week'})
    df["element_type"] = df["element_type"].map({1: 'GK', 2: 'DEF', 3: 'MID', 4: 'FWD'})
    return df

# def merge_player_data(player_data, raw_stats_df):
#     """Merge raw player stats with game week data."""
#     df = pd.DataFrame()
#     for _, row in raw_stats_df.iterrows():
#         player_id = row['id']
#         player_name = f"{row['first_name']}_{row['second_name']}_{player_id}"
#         symbol_name = normalize_name(player_name)
#         player_gw_data = player_data.get(symbol_name)
        
#         if player_gw_data is not None:
#             player_gw_data.reset_index(inplace=True)
#             player_gw_data['id'] = player_id
#             for col in row.index:
#                 player_gw_data[col] = row[col]
#             df = pd.concat([df, player_gw_data], ignore_index=True)
 
#     df = df[['element_type', 'team', 'second_name', 'first_name', 'id', 'total_points', 'value', 'index']]
#     df = df.rename(columns={'index': 'Game_Week'})
#     df["element_type"] = df["element_type"].map({1: 'GK', 2: 'DEF', 3: 'MID', 4: 'FWD'})
#     return df

def select_position(position, count, max_players_per_team, max_spend, players_df, current_players, current_spend, current_teams):
    """Select players for a specific position until the required count is reached."""
    current_player_ids = [player['id'] for player in current_players]
    current_teams = current_teams.copy()
    selected_players = []

    while count > 0 and not players_df.empty:
        player = players_df.sample().iloc[0]
        team_id = player['team']
        player_value = player['value']
 
        # Check constraints
        if (player['id'] not in current_player_ids) and \
           (current_teams.get(team_id, 0) < max_players_per_team) and \
           (current_spend + player_value <= max_spend):
            selected_players.append(player)
            current_spend += player_value
            current_teams[team_id] += 1
            current_player_ids.append(player['id'])
            count -= 1

    return selected_players

def select_random_team(team_structure, max_players_per_team, max_spend, player_data):
    """Select a random team of players based on position, budget, and team constraints."""
    total_spend = 0
    team_counts = Counter()
    selected_players = []
    
    for position, count in team_structure.items():
        players = player_data[player_data['element_type'] == position]
        players_selected = select_position(position, count, max_players_per_team, max_spend, players, selected_players, total_spend, team_counts)
        selected_players.extend(players_selected)
   
    return pd.DataFrame(selected_players)

# Constants
PLAYERS_DIR = './data/2023-24/players/'
RAW_DATA_PATH = './data/2023-24/players_raw.csv'
GAME_WEEK = 38
MAX_PLAYERS_PER_TEAM = 4
MAX_SPEND = 1000
RUNS = 100000
 
# Main execution
player_data = ingest_player_data(PLAYERS_DIR)
raw_stats_df = pd.read_csv(RAW_DATA_PATH)

# Merge player data
all_data_df = merge_player_data(player_data, raw_stats_df)

# Filter by recent game weeks
recent_gw_data = all_data_df[all_data_df['Game_Week'] >= GAME_WEEK - 5]
mean_total_points = recent_gw_data.groupby("id")['total_points'].mean().reset_index()

# Filter for the previous game week
game_week_data = all_data_df[all_data_df['Game_Week'] == GAME_WEEK - 1]

# Merge with average total points
merged_df = pd.merge(game_week_data, mean_total_points, on='id', how='left', suffixes=('', '_new'))
merged_df['average_total_points'] = merged_df['total_points_new'].fillna(merged_df['total_points'])
merged_df.drop(columns=['total_points_new'], inplace=True)

# Team Selection Simulation
team_structure = {'GK': 2, 'DEF': 5, 'MID': 5, 'FWD': 3}
all_teams = []
 
for run_id in range(RUNS):
    team_df = select_random_team(team_structure, MAX_PLAYERS_PER_TEAM, MAX_SPEND, merged_df)
    team_df['run_ID'] = run_id
    all_teams.append(team_df)

all_teams_df = pd.concat(all_teams, ignore_index=True)
total_points_per_run = all_teams_df.groupby('run_ID')['average_total_points'].sum()
best_run_id = total_points_per_run.idxmax()
best_team_df = all_teams_df[all_teams_df['run_ID'] == best_run_id]
total_spend_best_team = best_team_df['value'].sum()
 
# Display the best team
print("\nBest Team (Run ID with highest total points):")
print(tabulate(best_team_df, headers='keys', tablefmt='fancy_grid'))
print("\nTotal Points of Best Team:", best_team_df['average_total_points'].sum())
print("Total Spend on Best Team:", total_spend_best_team)
