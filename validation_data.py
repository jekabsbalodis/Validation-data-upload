# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "duckdb==1.3.2",
#     "pandas",
#     "python-dotenv",
#     "typer",
# ]
# ///

import typer
from typing_extensions import Annotated
from pathlib import Path
import duckdb
from dotenv import load_dotenv
import os

load_dotenv()
MD_TOKEN = os.getenv('MD_TOKEN')


def main(
    validation_files: Annotated[
        Path,
        typer.Argument(
            help='Path to the files containing validation data', show_default=False
        ),
    ],
) -> None:
    if validation_files.is_file() or not validation_files.exists():
        print('Please provide a path to a folder')
        raise typer.Exit()

    con = mdConnection()

    addMarsruts(con, validation_files)
    addParks(con, validation_files)
    addTranspVeids(con, validation_files)
    addTransports(con, validation_files)
    addTransportsMarsruts(con, validation_files)
    addValidacijas(con, validation_files)


def mdConnection() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(f'md:validacijas?motherduck_token={MD_TOKEN}')
    return con


def addMarsruts(con: duckdb.DuckDBPyConnection, files: Path) -> None:
    con.sql(f"""--sql
    INSERT OR IGNORE INTO metabase.Marsruts_lookup (Tmarsruts, MarsrNos)
    SELECT DISTINCT Tmarsruts, MarsrNos
    FROM read_csv('{files}/*')
    """)


def addParks(con: duckdb.DuckDBPyConnection, files: Path) -> None:
    con.sql(f"""--sql
    INSERT OR IGNORE INTO metabase.Parks_lookup (id, Parks)
    SELECT DISTINCT SUBSTRING(Parks, 1, 1) AS id, Parks
    FROM read_csv('{files}/*');
    """)


def addTranspVeids(con: duckdb.DuckDBPyConnection, files: Path) -> None:
    con.sql(f"""--sql
    INSERT OR IGNORE INTO metabase.TranspVeids_lookup (id, TranspVeids)
    SELECT ROW_NUMBER() OVER(ORDER BY TranspVeids) AS id, TranspVeids
    FROM (SELECT DISTINCT TranspVeids FROM read_csv('{files}/*'));
    """)


def addTransportsMarsruts(con: duckdb.DuckDBPyConnection, files: Path) -> None:
    con.sql(f"""--sql
    INSERT OR IGNORE INTO metabase.Transports_Marsruts_lookup (id, GarNr, TMarsruts)
    SELECT ROW_NUMBER() OVER(ORDER BY GarNr, TMarsruts) AS id, GarNr, TMarsruts
    FROM (SELECT DISTINCT GarNr, TMarsruts FROM read_csv('{files}/*'))
    """)


def addTransports(con: duckdb.DuckDBPyConnection, files: Path) -> None:
    con.sql(f"""--sql
    INSERT OR IGNORE INTO metabase.Transports_lookup (GarNr, TranspVeids_id, Parks_id)
    SELECT DISTINCT vf.GarNr, mtl.id, mpl.id
    FROM read_csv('{files}/*') vf
    JOIN metabase.TranspVeids_lookup mtl on vf.TranspVeids = mtl.TranspVeids
    JOIN metabase.Parks_lookup mpl on vf.Parks = mpl.Parks;
    """)


def addValidacijas(con: duckdb.DuckDBPyConnection, files: Path) -> None:
    con.sql(f"""--sql
    INSERT OR IGNORE INTO metabase.validacijas (GarNr, Virziens, ValidTalonaId, Laiks)
    SELECT GarNr, Virziens, ValidTalonaId, Laiks
    FROM read_csv('{files}/*')
    """)


if __name__ == '__main__':
    typer.run(main)
