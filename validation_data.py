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
from rich.progress import Progress, SpinnerColumn, TextColumn
from typing_extensions import Annotated

load_dotenv()
MD_TOKEN: str = os.getenv('MD_TOKEN', 'NO TOKEN FOUND')

read_csv_columns: str = """
    columns = {
        'Ier_ID': 'VARCHAR',
        'Parks': 'VARCHAR',
        'TranspVeids': 'VARCHAR',
        'GarNr': 'VARCHAR',
        'MarsrNos': 'VARCHAR',
        'TMarsruts': 'VARCHAR',
        'Virziens': 'VARCHAR',
        'ValidTalonaId': 'VARCHAR',
        'Laiks': 'TIMESTAMP'
    }"""


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

    with Progress(
        SpinnerColumn(),
        TextColumn('{task.description}'),
        transient=True,
    ) as progress:
        task_marsruts = progress.add_task(
            'Processing "Marsruts_lookup..."',
            total=None,
        )
        addMarsruts(con, validation_files, progress, task_marsruts)
        progress.remove_task(task_marsruts)

        task_transp_veids = progress.add_task(
            'Processing "TranspVeids_lookup..."',
            total=None,
        )
        addTranspVeids(con, validation_files, progress, task_transp_veids)
        progress.remove_task(task_transp_veids)

        task_parks = progress.add_task(
            'Processing "Parks_lookup..."',
            total=None,
        )
        addParks(con, validation_files, progress, task_parks)
        progress.remove_task(task_parks)

        task_transports = progress.add_task(
            'Processing "Transports_lookup..."',
            total=None,
        )
        addTransports(con, validation_files, progress, task_transports)
        progress.remove_task(task_transports)

        task_transports_marsruts = progress.add_task(
            'Processing "Transports_Marsruts_lookup..."',
            total=None,
        )
        addTransportsMarsruts(con, validation_files, progress, task_transports_marsruts)
        progress.remove_task(task_transports_marsruts)

        task_validacijas = progress.add_task(
            'Processing "Validacijas"...',
            total=None,
        )
        addValidacijas(con, validation_files, progress, task_validacijas)
        progress.remove_task(task_validacijas)


def mdConnection() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(f'md:validacijas?motherduck_token={MD_TOKEN}')
    # con = duckdb.connect('db.duckdb')
    con.sql("""--sql
        CREATE SCHEMA IF NOT EXISTS metabase;

        CREATE TABLE IF NOT EXISTS metabase.TranspVeids_lookup (
            id USMALLINT PRIMARY KEY,
            TranspVeids VARCHAR(15) NOT NULL UNIQUE);

        CREATE TABLE IF NOT EXISTS metabase.Parks_lookup (
            id USMALLINT PRIMARY KEY,
            Parks VARCHAR(10) NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS metabase.Marsruts_lookup (
            TMarsruts VARCHAR(10) PRIMARY KEY,
            MarsrNos VARCHAR(100) NOT NULL
        );

        CREATE TABLE IF NOT EXISTS metabase.Transports_lookup (
            GarNr VARCHAR(10) PRIMARY KEY,
            TranspVeids_id USMALLINT NOT NULL,
            Parks_id USMALLINT NOT NULL,
            FOREIGN KEY (TranspVeids_id) REFERENCES metabase.TranspVeids_lookup (id),
            FOREIGN KEY (Parks_id) REFERENCES metabase.Parks_lookup (id)
        );

        CREATE TABLE IF NOT EXISTS metabase.Transports_Marsruts_lookup (
            GarNr VARCHAR(10) NOT NULL,
            TMarsruts VARCHAR(10) NOT NULL,
            PRIMARY KEY (GarNr, TMarsruts),
            FOREIGN KEY (GarNr) REFERENCES metabase.Transports_lookup (GarNr),
            FOREIGN KEY (TMarsruts) REFERENCES metabase.Marsruts_lookup (TMarsruts)
        );

        CREATE TABLE IF NOT EXISTS metabase.validacijas (
            GarNr VARCHAR(10) NOT NULL,
            Virziens VARCHAR(10) NOT NULL,
            ValidTalonaId VARCHAR(10) NOT NULL,
            Laiks TIMESTAMP NOT NULL,
            PRIMARY KEY (GarNr, Virziens, ValidTalonaId, Laiks),
            FOREIGN KEY (GarNr) REFERENCES metabase.Transports_lookup (GarNr),
            UNIQUE (GarNr, Virziens, ValidTalonaId, Laiks)
        );
        """)
    return con


def addMarsruts(
    con: duckdb.DuckDBPyConnection, files: Path, progress: Progress, task
) -> None:
    con.sql(f"""--sql
        INSERT INTO metabase.Marsruts_lookup (Tmarsruts, MarsrNos)
        SELECT DISTINCT Tmarsruts, MarsrNos
        FROM read_csv('{files}/*',{read_csv_columns})
        WHERE TMarsruts IS NOT NULL
        AND MarsrNos IS NOT NULL
        AND LENGTH(TRIM(TMarsruts)) > 0
        AND LENGTH(TRIM(MarsrNos)) > 0
        AND TMarsruts NOT IN (SELECT TMarsruts FROM metabase.Marsruts_lookup)
        """)
    progress.update(task, completed=1)


def addParks(
    con: duckdb.DuckDBPyConnection, files: Path, progress: Progress, task
) -> None:
    con.sql(f"""--sql
        INSERT INTO metabase.Parks_lookup (id, Parks)
        SELECT DISTINCT SUBSTRING(Parks, 1, 1) AS id, Parks
        FROM read_csv('{files}/*',{read_csv_columns})
        WHERE Parks IS NOT NULL
        AND LENGTH(TRIM(Parks)) > 0
        AND Parks NOT IN (SELECT Parks FROM metabase.Parks_lookup)
        ORDER BY Parks;
        """)
    progress.update(task, completed=1)


def addTranspVeids(
    con: duckdb.DuckDBPyConnection, files: Path, progress: Progress, task
) -> None:
    con.sql(f"""--sql
        INSERT INTO metabase.TranspVeids_lookup (id, TranspVeids)
        SELECT ROW_NUMBER() OVER(ORDER BY TranspVeids) AS id, TranspVeids
        FROM (SELECT DISTINCT TranspVeids FROM read_csv('{files}/*',{read_csv_columns}))
        WHERE TranspVeids IS NOT NULL
        AND LENGTH(TRIM(TranspVeids)) > 0
        AND TranspVeids NOT IN (SELECT TranspVeids FROM metabase.TranspVeids_lookup)
        ORDER By TranspVeids;
        """)
    progress.update(task, completed=1)


def addTransportsMarsruts(
    con: duckdb.DuckDBPyConnection, files: Path, progress: Progress, task
) -> None:
    con.sql(f"""--sql
        INSERT INTO metabase.Transports_Marsruts_lookup (GarNr, TMarsruts)
        SELECT DISTINCT
            vf.GarNr,
            vf.TMarsruts
        FROM read_csv('{files}/*',{read_csv_columns}) vf
        JOIN metabase.Transports_lookup tl ON vf.GarNr = tl.GarNr
        JOIN metabase.Marsruts_lookup ml ON vf.TMarsruts = ml.TMarsruts
        WHERE vf.GarNr IS NOT NULL
        AND vf.TMarsruts IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM metabase.Transports_Marsruts_lookup tml
            WHERE tml.GarNr = vf.GarNr AND tml.TMarsruts = vf.TMarsruts
        )
        """)
    progress.update(task, completed=1)


def addTransports(
    con: duckdb.DuckDBPyConnection, files: Path, progress: Progress, task
) -> None:
    con.sql(f"""--sql
        INSERT INTO metabase.Transports_lookup (GarNr, TranspVeids_id, Parks_id)
        SELECT DISTINCT vf.GarNr, mtl.id, mpl.id
        FROM read_csv('{files}/*',{read_csv_columns}) vf
        JOIN metabase.TranspVeids_lookup mtl on vf.TranspVeids = mtl.TranspVeids
        JOIN metabase.Parks_lookup mpl on vf.Parks = mpl.Parks
        WHERE vf.GarNr IS NOT NULL
        AND vf.GarNr NOT IN (SELECT GarNr FROM metabase.Transports_lookup);
        """)
    progress.update(task, completed=1)


def addValidacijas(
    con: duckdb.DuckDBPyConnection, files: Path, progress: Progress, task
) -> None:
    con.sql(f"""--sql
        INSERT OR IGNORE INTO metabase.validacijas (GarNr, Virziens, ValidTalonaId, Laiks)
        SELECT
            vf.GarNr,
            vf.Virziens,
            vf.ValidTalonaId,
            vf.Laiks
        FROM read_csv('{files}/*',{read_csv_columns}) vf
        WHERE vf.GarNr IS NOT NULL
        AND vf.Virziens IS NOT NULL
        AND vf.ValidTalonaId IS NOT NULL
        AND vf.Laiks IS NOT NULL;
        """)
    progress.update(task, completed=1)


if __name__ == '__main__':
    typer.run(main)
