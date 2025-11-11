import codecs
import os
import zipfile
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import duckdb
import pytest
import requests
from prefect import flow
from prefect.testing.utilities import prefect_test_harness

from validation_data import (
    check_data_encoding,
    cleanup_temp_directories,
    close_connection,
    connect_duckdb,
    convert_data,
    download_data,
    extract_downloaded_data,
    get_md_token,
    update_db,
    validate_downloaded_data,
    write_data,
)


@pytest.fixture(autouse=True, scope='session')
def prefect_test_fixture():
    """Enable Prefect test mode for all tests"""
    with prefect_test_harness():
        yield


@pytest.fixture
def sample_csv():
    """Sample csv for testing"""
    return """Ier_ID,Parks,TranspVeids,GarNr,MarsrNos,TMarsruts,Virziens,ValidTalonaId,Laiks
175426,2 parks,Trolejbuss,29299,Centrāltirgus - Mežciems,Tr35,Forth,3645190,01.09.2025 00:02:20
192554,2 parks,Trolejbuss,29320,Centrāltirgus - Berģuciems,Tr31,Forth,3626095,01.09.2025 00:08:33
203671,7 parks,Autobuss,71084,Abrenes iela -Mežaparks,A9,Forth,3433289,01.09.2025 00:55:03
203744,7 parks,Autobuss,71084,Abrenes iela -Mežaparks,A9,Forth,3623389,01.09.2025 03:38:02
204166,7 parks,Autobuss,71084,Abrenes iela -Mežaparks,A9,Forth,3623009,01.09.2025 01:27:06
"""  # noqa: E501


@pytest.fixture
def temp_zip_file(sample_csv):
    """Create a temporary zip file with CSV data"""
    temp_csv_dir = TemporaryDirectory()
    csv_path = Path(temp_csv_dir.name) / 'data.csv'

    with open(csv_path, 'w', encoding='utf-8') as f:
        f.write(sample_csv)

    temp_zip_dir = TemporaryDirectory()
    zip_path = Path(temp_zip_dir.name) / 'data.zip'
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.write(csv_path, arcname='data.csv')

    temp_csv_dir.cleanup()
    yield temp_zip_dir, zip_path
    temp_zip_dir.cleanup()


@pytest.fixture
def temp_zip_file_windows_encoding(sample_csv):
    """Create a temporary zip file with windows-1257 encoded CSV"""
    temp_csv_dir = TemporaryDirectory()
    csv_path = Path(temp_csv_dir.name) / 'data.csv'

    with codecs.open(str(csv_path), 'w', encoding='windows-1257') as f:
        f.write(sample_csv)

    temp_zip_dir = TemporaryDirectory()
    zip_path = Path(temp_zip_dir.name) / 'data.zip'
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.write(csv_path, arcname='data.csv')

    temp_csv_dir.cleanup()
    yield temp_zip_dir, zip_path
    temp_zip_dir.cleanup()


class TestGetMdToken:
    """Tests for get_md_token task"""

    def test_get_md_token_is_prefect_task(self):
        from prefect import Task  # noqa: PLC0415

        assert isinstance(get_md_token, Task)

    @pytest.mark.asyncio
    async def test_get_md_token_success(self):
        """Test successful token retrieval"""
        mock_secret = MagicMock()
        mock_secret.get.return_value = 'test_token_123'
        async_mock_load = AsyncMock(return_value=mock_secret)

        with patch('validation_data.Secret.load', async_mock_load):
            token = await get_md_token()
            assert token == 'test_token_123'
            async_mock_load.assert_awaited_once_with('md-token')
            mock_secret.get.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_md_token_returns_string(self):
        mock_secret = MagicMock()
        mock_secret.get.return_value = '12345'
        async_mock_load = AsyncMock(return_value=mock_secret)

        with patch('validation_data.Secret.load', async_mock_load):
            token = await get_md_token()
            assert isinstance(token, str)

    @pytest.mark.asyncio
    async def test_get_md_token_failure(self):
        """Test token retrieval failure"""
        async_mock_load = AsyncMock(side_effect=ValueError('Secret not found'))

        with patch('validation_data.Secret.load', async_mock_load):
            with pytest.raises(ValueError):
                await get_md_token()

    @pytest.mark.asyncio
    async def test_get_md_token_incorrect(self):
        """Test token retrieval with incorrect token"""
        mock_secret = MagicMock()
        mock_secret.get.return_value = 'incorrect_token_123'
        async_mock_load = AsyncMock(return_value=mock_secret)

        with patch('validation_data.Secret.load', async_mock_load):
            token = await get_md_token()
            assert token != 'test_token_123'
            async_mock_load.assert_awaited_once_with('md-token')
            mock_secret.get.assert_called_once()


class TestDownloadData:
    """Tests for download_data task"""

    def test_download_data_success(self):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.iter_content.return_value = [b'data']
        mock_response.raise_for_status.return_value = None

        with patch('validation_data.requests.get', return_value=mock_response):
            result = download_data(url='https://example.com/data.zip')

            files = os.listdir(result.name)

            assert isinstance(result, TemporaryDirectory)
            assert os.path.exists(result.name)
            assert len(files) == 1
            assert files[0] == 'data.zip'

            result.cleanup()

    def test_download_data_http_error(self):
        """Test download_data with http error"""
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.HTTPError('404 Not Found')

        with patch('validation_data.requests.get', return_value=mock_response):
            with pytest.raises(requests.HTTPError):
                download_data(url='https://example.com/data.zip')

    def test_download_data_invalid_url_format(self):
        """Test download_data failure due to invalid URL format"""
        with pytest.raises(ValueError):
            download_data(url='ftp://example.com/data.zip')


class TestValidateDownloadedData:
    """Tests for validate_downloaded_data task"""

    def test_validate_downloaded_data_valid_single_zip_file(self, temp_zip_file):
        """Test validate_downloaded_data with a valid single zip file"""
        download_dir, zip_path = temp_zip_file
        result = validate_downloaded_data(download_dir=download_dir)
        assert result == zip_path

    def test_validate_downloaded_data_no_files(self):
        """Test validate_downloalded_data with no files"""
        download_dir = TemporaryDirectory()
        with pytest.raises(
            ValueError, match='No files found in the download directory'
        ):
            validate_downloaded_data(download_dir=download_dir)
        download_dir.cleanup()

    def test_validate_downloaded_data_multiple_files(self, temp_zip_file):
        """Test validate_downloaded_data with multiple files"""
        download_dir, zip_path = temp_zip_file
        extra_file_path = Path(download_dir.name) / 'extra_file.txt'
        with open(extra_file_path, 'w') as f:
            f.write('Text in the extra file.')
        with pytest.raises(ValueError, match='Expected 1 file, found 2'):
            validate_downloaded_data(download_dir=download_dir)

    def test_validate_downloaded_data_non_zip_file(self):
        """Test validate_downloaded_data with a non-zip file"""
        download_dir = TemporaryDirectory()
        non_zip_file_path = Path(download_dir.name) / 'non_zip_file.txt'
        with open(non_zip_file_path, 'w') as f:
            f.write('Text in the non_zip_file.')
        with pytest.raises(ValueError, match='Downloaded file is not a zip file'):
            validate_downloaded_data(download_dir=download_dir)
        download_dir.cleanup()


class TestExtractDownloadedData:
    """Tests for extract_downloaded_data"""

    def test_extract_downloaded_data_success(self, temp_zip_file):
        """Test extract_downloaded_data with a valid zip file"""
        _, zip_path = temp_zip_file
        extract_dir = extract_downloaded_data(zip_path=zip_path)

        assert isinstance(extract_dir, TemporaryDirectory)
        assert os.path.exists(extract_dir.name)

        extracted_files = os.listdir(extract_dir.name)
        assert len(extracted_files) == 1
        assert 'data.csv' in extracted_files

        extract_dir.cleanup()


class TestConnectDuckdb:
    """Tests for connect_duckdb task"""

    def test_connect_duckdb_in_memory(self):
        """Test connect_duckdb with in_memory=True"""
        con = connect_duckdb()
        assert isinstance(con, duckdb.DuckDBPyConnection)
        con.close()

    def test_connect_duckdb_motherduck(self):
        """Test connect_duckdb with in_memory=False and a valid token"""
        mock_con = MagicMock(spec=duckdb.DuckDBPyConnection)
        with patch('duckdb.connect', return_value=mock_con):
            con = connect_duckdb(in_memory=False, token='valid_token')
            assert con == mock_con
            mock_con.execute.assert_called_once()

    def test_connect_duckdb_table_creation(self):
        """Test connect_duckdb creates the validacijas table with the correct schema"""
        mock_con = MagicMock(spec=duckdb.DuckDBPyConnection)
        with patch('duckdb.connect', return_value=mock_con):
            connect_duckdb()
            mock_con.execute.asser_called_once_with("""--sql
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

    def test_connect_duckdb_invalid_token(self):
        """Test connect_duckdb raises an exception with an invalid token"""
        with pytest.raises(duckdb.InvalidInputException):
            connect_duckdb(in_memory=False, token='invalid_token')


class TestConvertData:
    """Tests for convert_data task"""

    def test_convert_data_success(self, temp_zip_file_windows_encoding):
        """Test convert_data with a valid windows-1257 encoded file"""
        _, zip_path = temp_zip_file_windows_encoding
        extract_dir = extract_downloaded_data(zip_path=zip_path)

        converted_dir = convert_data(extracted_path=extract_dir)

        assert isinstance(converted_dir, TemporaryDirectory)
        assert os.path.exists(converted_dir.name)

        converted_files = os.listdir(converted_dir.name)
        assert len(converted_files) == 1
        assert 'data.csv' in converted_files

        converted_file_path = Path(converted_dir.name) / 'data.csv'
        with open(converted_file_path, encoding='utf-8') as f:
            contents = f.read()
            assert 'Centrāltirgus - Mežciems' in contents

        extract_dir.cleanup()
        converted_dir.cleanup()


class TestCheckDataEncoding:
    """Test for check_data_encoding task"""

    def test_check_data_encoding_utf8_encoding(self, temp_zip_file):
        """Test check_data_encoding with utf-8 encoded files"""
        _, temp_zip_path = temp_zip_file
        extract_path = extract_downloaded_data(zip_path=temp_zip_path)
        result = check_data_encoding(extracted_path=extract_path)
        assert result == extract_path

    def test_check_data_encoding_windows1257_encoding(
        self, temp_zip_file_windows_encoding
    ):
        _, temp_zip_path = temp_zip_file_windows_encoding
        extract_path = extract_downloaded_data(zip_path=temp_zip_path)
        result = check_data_encoding(extracted_path=extract_path)

        assert result != extract_path
        assert isinstance(result, TemporaryDirectory)
        assert os.path.exists(result.name)

        result.cleanup()


class TestWriteData:
    """Tests for write_data task"""

    def test_write_data_success(self, temp_zip_file):
        """Test write_data with a valid zip"""
        _, zip_path = temp_zip_file
        extracted_dir = extract_downloaded_data(zip_path=zip_path)
        validated_dir = check_data_encoding(extracted_path=extracted_dir)
        con = connect_duckdb()

        write_data(con=con, data_dir=validated_dir)

        result = con.execute("""--sql
            select count(*) from validacijas
            """).fetchone()[0]
        assert result == 5
        con.close()

    def test_write_duplicate_data(self, temp_zip_file):
        """Test write_data with duplicate data"""
        _, zip_path = temp_zip_file
        extracted_dir = extract_downloaded_data(zip_path=zip_path)
        validated_dir = check_data_encoding(extracted_path=extracted_dir)
        con = connect_duckdb()

        write_data(con=con, data_dir=validated_dir)
        write_data(con=con, data_dir=validated_dir)

        result = con.execute("""--sql
            select count(*) from validacijas
            """).fetchone()[0]
        assert result == 5
        con.close()
