from pathlib import Path
import pandas as pd
import csv

data_path = Path("dataverse_files/communication.csv")
supervisors_path = Path("dataverse_files/reportsto.csv")


def read_df(path: Path):
    df = pd.read_csv(data_path, sep=";", header=0)
    df.EventDate = pd.to_datetime(df.EventDate)
    return df


def filter_teams(path: Path) -> dict[int, list[int]]:
    teams = dict()
    with path.open() as file:
        reader = csv.DictReader(file, delimiter=";")
        for row in reader:
            try: 
                supervisor_id = int(row['ReportsToID'])
                team_member_id = int(row['ID'])
                if supervisor_id not in teams:
                    teams[supervisor_id] = []
                teams[supervisor_id].append(team_member_id)
            except ValueError:
                pass

    return teams


def prepare_cascades(supervisor: str, team: list[str], df: pd.DataFrame):
    team_df = df[df['Sender'].isin(team) | df['Recipient'].isin(team)]
    team_df = team_df.groupby(["EventDate"])["EventDate"].count().reset_index(name="magnitude")
    team_df["time"] = team_df["EventDate"].view('int64') / 10**9
    t0 = min(team_df["time"])
    tmax = max(team_df["time"])
    team_df["time"] = team_df["time"].apply(lambda x: (100*(x-t0))/(tmax-t0))
    team_df[["magnitude", "time"]].to_csv(f"data/{supervisor}.csv", index=False, sep=";")


def prepare_full_dataset(df: pd.DataFrame):
    df = df.groupby(["EventDate"])["EventDate"].count().reset_index(name="magnitude")
    df["time"] = df["EventDate"].view('int64') / 10**9
    t0 = min(df["time"])
    tmax = max(df["time"])
    df["time"] = df["time"].apply(lambda x: (100*(x-t0))/(tmax-t0))
    df[["magnitude", "time"]].to_csv(f"data/full.csv", index=False, sep=";")


def run():
    df = read_df(data_path)
    teams = filter_teams(supervisors_path)
    for team in teams:
        prepare_cascades(team, [team] + teams[team], df)
    #prepare_full_dataset(df)


if __name__ == "__main__":
    run()
