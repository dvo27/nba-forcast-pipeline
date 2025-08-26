# Fetch game stats & attendance via nba_api
from datetime import date, timedelta
import time
import pandas as pd
from nba_api.stats.endpoints import scoreboardv2
from nba_api.stats.static import teams


OUT_DIR = 'data/raw'

def fetch_scoreboard_by_date(date: str) -> pd.DataFrame:
    """
    Given a date formatted by 'YYYY-MM-DD', return a dataframe of the scoreboard(s) for NBA games played
    on that day

    Args:
        date (str): A date formatted by 'YYYY-MM-DD'

    Returns:
        pd.DataFrame: Dataframe containing scoreboards of games played on given date
    """
    sb = scoreboardv2.ScoreboardV2(game_date=date, day_offset=0)

    game_header = sb.game_header.get_data_frame()
    team_leaders = sb.team_leaders.get_data_frame()

    return (game_header, team_leaders)


def get_static_team_map() -> dict[int, str]:
    """
    Build a stable fallback mapping of TEAM_ID → TEAM_ABBREVIATION.

    Returns a dict like {1610612738: "BOS", ...} using nba_api.stats.static.teams
    """
    all_teams = teams.get_teams()
    
    return {team['id']: team['abbreviation'] for team in all_teams}
    
    
def attach_tricodes(header_df: pd.DataFrame, team_leaders_df: pd.DataFrame, id_to_tri: dict[int,str]) -> pd.DataFrame:
  """Return one-row-per-game with home_tricode and away_tricode added.

  Args:
      header_df (pd.DataFrame): Dataframe of game_header
      team_leaders_df (pd.DataFrame): Dataframe of team_leaders
      id_to_tri (dict[int,str]): Fallback mapping of team id to team abbreviations

  Returns:
      pd.DataFrame: _description_
  """
  
def fetch_tipoff_utc(game_id: str) -> tuple[str | None, int | None]:
    return

def build_schedule_rows(header_with_tricodes_df: pd.DataFrame, run_ts: int, source_date: str) -> list[dict]:
    return

def fetch_nba_schedule(start_date: str, end_date: str) -> pd.DataFrame:
    return

def save_schedule(df: pd.DataFrame, out_dir: str = OUT_DIR, run_ts: int | None = None) -> str:
    return


def fetch_nba_stats(start_year: int, start_month: int, start_day: int, end_year: int, end_month: int, end_day: int) -> pd.DataFrame:
    """
    - Use nba_api to pull game logs for scoreboards during the 2024-2025 NBA PLayoffs
    - Extract games within the playoff period (Apr 19, 2025 00:00:00 UTC - Jun 23, 2025 23:59:59 UTC)
    - For each game, extract:
      - game_id, game_date, home_team, away_team, home_pts, away_pts, attendance, tipoff_utc
    - Save game data to data/raw/nba_schedule_<run_ts>.csv
    - Return a DataFrame of game data
    """

    # Make sure end date is ahead of start date

    # Capture search runtime
    run_ts = time.time()

    # NBA Playoff Duration Start: (Apr 19, 2025 00:00:00 UTC)
    start_date = date(start_year, start_month, start_day)

    # NBA Playoff Duration End:  (Jun 23, 2025 23:59:59 UTC)
    end_date = date(end_year, end_month, end_day)

    playoff_data = []

    # Boolean to check whether game date is within playoff duration
    # in_window = start_utc <= int(submission.created_utc) <= end_utc

    # What we need to keep track of:
    """
    From the GameHeader table:

    GAME_ID → unique game id

    GAMECODE → useful for debugging

    GAME_STATUS_ID, GAME_STATUS_TEXT (e.g., “7:00 PM ET”, “Final”)

    HOME_TEAM_ID, VISITOR_TEAM_ID

    GAME_DATE_EST (date component) 
    hoopr.sportsdataverse.org

    From the LineScore table:

    GAME_ID, TEAM_ID, TEAM_ABBREVIATION (use this to attach home/away tricodes)

    """
    game_header = scoreboardv2.ScoreboardV2(
        game_date=str(start_date)).get_data_frames()[0]
    team_leaders = scoreboardv2.ScoreboardV2(
        game_date=str(start_date)).get_data_frames()[7]

    # we need to assign team abbreviations from the team leaders table to each team correspinding with team_id per game row

    # 1) Create lookup from team_leaders
    tl = team_leaders[["GAME_ID", "TEAM_ID",
                       "TEAM_ABBREVIATION"]].drop_duplicates()

    # 2) Correctly format datatypes to align so merge doesn't fail
    game_header["HOME_TEAM_ID"] = game_header["HOME_TEAM_ID"].astype("int64")
    game_header["VISITOR_TEAM_ID"] = game_header["VISITOR_TEAM_ID"].astype(
        "int64")
    tl["TEAM_ID"] = tl["TEAM_ID"].astype("int64")

    # 3) Merge HOME tricode
    home_map = tl.rename(columns={
        "TEAM_ID": "HOME_TEAM_ID",
        "TEAM_ABBREVIATION": "home_tricode"
    })

    gh = pd.merge(
        game_header,
        home_map[["GAME_ID", "HOME_TEAM_ID", "home_tricode"]],
        on=["GAME_ID", "HOME_TEAM_ID"],
        how="left",
        validate="1:1"
    )

    # 4) Merge AWAY tricode
    away_map = tl.rename(columns={
        "TEAM_ID": "VISITOR_TEAM_ID",
        "TEAM_ABBREVIATION": "away_tricode"
    })

    gh = pd.merge(
        gh,
        away_map[["GAME_ID", "VISITOR_TEAM_ID", "away_tricode"]],
        on=["GAME_ID", "VISITOR_TEAM_ID"],
        how="left",
        validate="1:1"
    )

    print(gh)

    # We need to loop thru dates and collect stats for each game played by each day
    time_delta = end_date - start_date

    for i in range(time_delta.days + 1):
        day = start_date + timedelta(days=i)

        sb = scoreboardv2.ScoreboardV2(game_date=str(day)).get_data_frames()
        game_header = sb.game_header.get_data_frame()
        team_leaders = sb.team_leaders.get_data_frame()

        game_data = {
            'GAME_ID': game_header['GAME_ID'],
            'GAMECODE': game_header['GAMECODE'],
            'GAME_STATUS_ID': game_header['GAME_STATUS_ID'],
            'GAME_STATUS_TEXT': game_header['GAME_STATUS_TEXT'],
            'HOME_TEAM_ID': game_header['HOME_TEAM_ID'],
            'VISITOR_TEAM_ID': game_header['VISITOR_TEAM_ID'],
            'GAME_DATE_EST': game_header['GAME_DATE_EST']
        }


if __name__ == '__main__':
    # fetch_nba_stats(2025, 4, 19, 2025, 6, 22)
    scoreboards = fetch_scoreboard_by_date('2025-06-22')
    

    
