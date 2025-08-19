# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "duckdb==1.3.2",
#     "pandas",
#     "python-dotenv",
#     "typer",
# ]
# ///

import os
from pathlib import Path

import duckdb
import typer
from dotenv import load_dotenv
from rich.progress import Progress  # Import Progress from rich
from typing_extensions import Annotated

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
    files = list(validation_files.glob('*'))

    with Progress() as progress:
        task_marsruts = progress.add_task(
            'Processing "Marsruts_lookup..."', total=len(files)
        )
        task_parks = progress.add_task('Processing "Parks_lookup"...', total=len(files))
        task_transp_veids = progress.add_task(
            'Processing "TranspVeids_lookup"...', total=len(files)
        )
        task_transports = progress.add_task(
            'Processing "Transports_lookup"...', total=len(files)
        )
        task_transports_marsruts = progress.add_task(
            'Processing "Transports_Marsruts_lookup"...', total=len(files)
        )
        task_validacijas = progress.add_task(
            'Processing "Validacijas"...', total=len(files)
        )

        addMarsruts(con, files, progress, task_marsruts)
        addParks(con, files, progress, task_parks)
        addTranspVeids(con, files, progress, task_transp_veids)
        addTransports(con, files, progress, task_transports)
        addTransportsMarsruts(con, files, progress, task_transports_marsruts)
        addValidacijas(con, files, progress, task_validacijas)


def mdConnection() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(f'md:validacijas?motherduck_token={MD_TOKEN}')
    return con


def addMarsruts(
    con: duckdb.DuckDBPyConnection, files: list[Path], progress: Progress, task
) -> None:
    for file in files:
        con.sql(f"""--sql
        INSERT OR IGNORE INTO metabase.Marsruts_lookup (Tmarsruts, MarsrNos)
        SELECT DISTINCT Tmarsruts, MarsrNos
        FROM read_csv('{file}')
        """)
        progress.update(task, advance=1)


def addParks(
    con: duckdb.DuckDBPyConnection, files: list[Path], progress: Progress, task
) -> None:
    for file in files:
        con.sql(f"""--sql
        INSERT OR IGNORE INTO metabase.Parks_lookup (id, Parks)
        SELECT DISTINCT SUBSTRING(Parks, 1, 1) AS id, Parks
        FROM read_csv('{file}');
        """)
        progress.update(task, advance=1)


def addTranspVeids(
    con: duckdb.DuckDBPyConnection, files: list[Path], progress: Progress, task
) -> None:
    for file in files:
        con.sql(f"""--sql
        INSERT OR IGNORE INTO metabase.TranspVeids_lookup (id, TranspVeids)
        SELECT ROW_NUMBER() OVER(ORDER BY TranspVeids) AS id, TranspVeids
        FROM (SELECT DISTINCT TranspVeids FROM read_csv('{file}'));
        """)
        progress.update(task, advance=1)


def addTransportsMarsruts(
    con: duckdb.DuckDBPyConnection, files: list[Path], progress: Progress, task
) -> None:
    for file in files:
        con.sql(f"""--sql
        INSERT OR IGNORE INTO metabase.Transports_Marsruts_lookup (id, GarNr, TMarsruts)
        SELECT ROW_NUMBER() OVER(ORDER BY GarNr, TMarsruts) AS id, GarNr, TMarsruts
        FROM (SELECT DISTINCT GarNr, TMarsruts FROM read_csv('{file}'))
        """)
        progress.update(task, advance=1)


def addTransports(
    con: duckdb.DuckDBPyConnection, files: list[Path], progress: Progress, task
) -> None:
    for file in files:
        con.sql(f"""--sql
        INSERT OR IGNORE INTO metabase.Transports_lookup (GarNr, TranspVeids_id, Parks_id)
        SELECT DISTINCT vf.GarNr, mtl.id, mpl.id
        FROM read_csv('{file}') vf
        JOIN metabase.TranspVeids_lookup mtl on vf.TranspVeids = mtl.TranspVeids
        JOIN metabase.Parks_lookup mpl on vf.Parks = mpl.Parks;
        """)
        progress.update(task, advance=1)


def addValidacijas(
    con: duckdb.DuckDBPyConnection, files: list[Path], progress: Progress, task
) -> None:
    for file in files:
        con.sql(f"""--sql
        INSERT OR IGNORE INTO metabase.validacijas (GarNr, Virziens, ValidTalonaId, Laiks)
        SELECT GarNr, Virziens, ValidTalonaId, Laiks
        FROM read_csv('{file}')
        """)
        progress.update(task, advance=1)


if __name__ == '__main__':
    typer.run(main)
