# Fetch game stats & attendance via nba_api
import pandas as pd
from nba_api.stats.endpoints import boxscoretraditionalv3

def fetch_nba_stats(season: str) -> pd.DataFrame:
    """
    - Use nba_api to pull game logs for all teams in `season` (e.g. "2024-25").
    - Extract games within the playoff period (Apr 19, 2025 00:00:00 UTC - Jun 23, 2025 23:59:59 UTC)
    - For each game, extract: 
      - game_id, game_date, home_team, away_team, home_pts, away_pts, attendance, tipoff_utc
    - Save game data to data/raw/nba_schedule_<run_ts>.csv
    - Return a DataFrame of game data
    """

