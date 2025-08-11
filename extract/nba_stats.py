# Fetch game stats & attendance via nba_api
import pandas as pd
from nba_api.stats.endpoints import boxscoretraditionalv3

def fetch_nba_stats(season: str) -> pd.DataFrame:
    """
    - Use nba_api to pull game logs for all teams in `season` (e.g. "2024-25").
    - For each game, extract: game_id, game_date, home_team, away_team,
      home_pts, away_pts, attendance.
    - Return a DataFrame.
    """

