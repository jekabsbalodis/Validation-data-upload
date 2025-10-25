import os
from pathlib import Path
from typing import Annotated

import duckdb
import typer
from dotenv import load_dotenv
from rich.progress import track

load_dotenv()
MD_TOKEN: str = os.getenv('MD_TOKEN', 'NO TOKEN FOUND')

read_csv_columns: str = """
    columns = {
        'Ier_ID': 'UINTEGER',
        'Parks': 'VARCHAR',
        'TranspVeids': 'VARCHAR',
        'GarNr': 'UINTEGER',
        'MarsrNos': 'VARCHAR',
        'TMarsruts': 'VARCHAR',
        'Virziens': 'VARCHAR',
        'ValidTalonaId': 'UINTEGER',
        'Laiks': 'TIMESTAMP'
    }"""


def main(
    validation_files: Annotated[
        Path,
        typer.Argument(
            help='Path to the files containing validation data', show_default=False
        ),
    ],
    local: Annotated[
        bool,
        typer.Option(
            '--local/--remote',
            '-l/-r',
            help='By default, uses a local DuckDB database "local.duckdb"',
            show_default='local',
        ),
    ] = True,
) -> None:
    if validation_files.is_file() or not validation_files.exists():
        print('Please provide a path to a folder')
        raise typer.Exit()

    con = md_connection(token=MD_TOKEN, local=local)

    read_csv_files(con, validation_files)


def md_connection(local: bool, token: str) -> duckdb.DuckDBPyConnection:
    if local:
        con = duckdb.connect('local.duckdb')
    else:
        con = duckdb.connect(f'md:validacijas?motherduck_token={token}')
    con.execute("""--sql
                create table if not exists validacijas (
                  Ier_ID UINTEGER,
                  Parks VARCHAR,
                  TranspVeids VARCHAR,
                  GarNr UINTEGER,
                  MarsrNos VARCHAR,
                  TMarsruts VARCHAR,
                  Virziens VARCHAR,
                  ValidTalonaId UINTEGER,
                  Laiks TIMESTAMP,
                  primary key (
                    Laiks,
                    Ier_ID
                  )
                );
                """)
    return con


def read_csv_files(
    con: duckdb.DuckDBPyConnection,
    files: Path,
) -> None:
    file_list = list(files.glob('*.csv')) + list(files.glob('*.txt'))

    for file in track(file_list, description='Raksta failus DuckDB...'):
        try:
            con.execute(
                f"""--sql
                    insert or ignore into validacijas
                    select * from read_csv(?,{read_csv_columns},escape='"');
                """,
                [str(file.as_posix())],
            )
        except duckdb.InvalidInputException:
            import codecs  # noqa: PLC0415

            with codecs.open(
                str(file.as_posix()),
                mode='r',
                encoding='windows-1257',
            ) as source:
                with codecs.open(
                    'target.txt',
                    mode='w',
                    encoding='utf-8',
                ) as target:
                    contents = source.read()
                    target.write(contents)

            con.execute(
                f"""--sql
                    insert or ignore into validacijas
                    select * from read_csv(?,{read_csv_columns},escape='"');
                """,
                ['target.txt'],
            )


if __name__ == '__main__':
    typer.run(main)
