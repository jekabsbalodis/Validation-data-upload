import codecs
import os
import zipfile
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Final

import duckdb
import requests
from prefect import flow, get_run_logger, task
from prefect.blocks.system import Secret
from prefect.cache_policies import NO_CACHE
from pydantic import HttpUrl

READ_CSV_COLUMNS: Final[str] = """
{
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


@task
async def get_md_token() -> str:
    """Retrieve MotherDuck token from Prefect secret"""
    prefect_secret = await Secret.load('md-token')
    return prefect_secret.get()


@task
def download_data(*, url: HttpUrl) -> TemporaryDirectory:
    """
    Download the zip file of raw validation data from data.gov.lv to temporary directory
    """
    download_dir = TemporaryDirectory()

    response = requests.get(
        url=url,
        stream=True,
    )
    response.raise_for_status()

    filename = str(url).split('/')[-1]
    filepath = os.path.join(download_dir.name, filename)
    with open(filepath, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    return download_dir


@task
def validate_downloaded_data(*, download_dir: TemporaryDirectory) -> Path:
    """Validate that the downloaded file is a single zip file"""
    files = os.listdir(download_dir.name)

    if len(files) == 0:
        raise ValueError('No files found in the download directory')
    elif len(files) > 1:
        raise ValueError(f'Expected 1 file, found {len(files)}: {files}')

    file_path: Path = Path(download_dir.name) / files[0]
    if not zipfile.is_zipfile(file_path):
        raise ValueError(f'Downloaded file is not a zip file: {file_path.name}')

    return file_path


@task
def extract_downloaded_data(*, zip_path: Path) -> TemporaryDirectory:
    """Extract the zip file to a temporary directory"""
    extract_dir = TemporaryDirectory()

    with zipfile.ZipFile(zip_path, 'r') as zip_file:
        zip_file.extractall(extract_dir.name)

    return extract_dir


@task
def connect_duckdb(
    *,
    token: str | None = None,
    in_memory: bool = True,
) -> duckdb.DuckDBPyConnection:
    """
    Create either in memory duckdb connection
    or MotherDuck connection for writing csv data
    """
    if in_memory:
        con = duckdb.connect()
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


@task
def convert_data(*, extracted_path: TemporaryDirectory) -> TemporaryDirectory:
    """Convert files from windows-1257 to utf-8 encoding"""
    files_path = Path(extracted_path.name)
    file_list = list(files_path.glob('*.csv')) + list(files_path.glob('*.txt'))

    converted_dir = TemporaryDirectory()
    converted_path = converted_dir.name

    for file in file_list:
        converted_file = converted_path / file.name

        with codecs.open(
            str(file.as_posix()),
            mode='r',
            encoding='windows-1257',
        ) as source:
            with codecs.open(
                str(converted_file),
                mode='w',
                encoding='utf-8',
            ) as target:
                contents = source.read()
                target.write(contents)

    return converted_dir


@task
def test_data(
    *,
    extracted_path: TemporaryDirectory,
) -> None | TemporaryDirectory:
    """Test if files are readable by duckdb and convert them if necessary"""
    try:
        files_glob: str = str(Path(extracted_path.name) / '**' / '*.*')
        test_con = duckdb.connect()
        test_con.execute(
            f"""--sql
             select count(*) from read_csv(?, columns={READ_CSV_COLUMNS}, escape='"')
             """,
            [files_glob],
        )
        test_con.close()
        return extracted_path

    except duckdb.InvalidInputException:
        converted_dir = convert_data(extracted_path=extracted_path)
        verify_con = duckdb.connect()
        converted_files_glob: str = str(Path(converted_dir.name) / '**' / '*.*')
        verify_con.execute(
            f"""--sql
             select count(*) from read_csv(?, columns={READ_CSV_COLUMNS}, escape='"')
             """,
            [converted_files_glob],
        )
        verify_con.close()
        return converted_dir


@task(cache_policy=NO_CACHE)
def write_data(
    *,
    con: duckdb.DuckDBPyConnection,
    data_dir: TemporaryDirectory,
) -> None:
    """Write csv data to duckdb database"""

    files_glob = str(Path(data_dir.name) / '**' / '*.*')

    con.execute(
        f"""--sql
        insert or ignore into validacijas
        select * from read_csv(?,columns={READ_CSV_COLUMNS},escape='"');
        """,
        [files_glob],
    )


@task(cache_policy=NO_CACHE)
def close_connection(*, con: duckdb.DuckDBPyConnection) -> None:
    """Close duckdb connection"""
    con.close()


@task
def cleanup_temp_directories(*, dirs: list[TemporaryDirectory]) -> None:
    """Cleanup created temporary directories"""
    for temp_dir in dirs:
        temp_dir.cleanup()


@flow
async def update_db(*, data_url: HttpUrl) -> None:
    """
    Main flow to download data,
    validate downloaded data,
    check data files' encoding and convert if necessary,
    testing data write to in-memory database,
    write data to MotherDuck
    """
    logger = get_run_logger()

    logger.info('Downloading and extracting data')

    download_dir = download_data(url=data_url)
    zip_path = validate_downloaded_data(download_dir=download_dir)
    extract_dir = extract_downloaded_data(zip_path=zip_path)

    logger.info("Validating data files' encoding")

    validated_dir = test_data(extracted_path=extract_dir)

    logger.info('Testing writing data to in-memory database')

    test_con = connect_duckdb()
    write_data(con=test_con, data_dir=validated_dir)
    close_connection(con=test_con)

    logger.info('Writing data to Motherduck')

    motherduck_token = await get_md_token()
    md_con = connect_duckdb(token=motherduck_token, in_memory=False)
    write_data(con=md_con, data_dir=validated_dir)
    close_connection(con=md_con)

    logger.info('Cleaning up temporary directories')

    cleanup_temp_directories(dirs=[download_dir, extract_dir, validated_dir])


if __name__ == '__main__':
    update_db.serve(
        name='validation-upload-test', parameters={'data_url': 'example.com'}
    )
