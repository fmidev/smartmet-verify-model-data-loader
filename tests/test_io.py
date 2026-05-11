"""Tests for I/O-dependent functions in _core, using mocks."""

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
import requests

from smartmet_verify_model_data_loader._core import (
    Config,
    fetch_instance_data,
    fetch_with_retry,
    get_instances,
    get_loaded_analysis_times,
    get_producer_id,
    get_stations,
    load_config,
    load_to_db,
    validate_params,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _cfg(**kwargs: object) -> Config:
    defaults: dict[str, object] = {
        "server_url": "https://smartmet.example.com",
        "edr_collection": "gfs_surface",
        "verif_producer": "gfs",
        "parameters": "Temperature,WindSpeedMS",
        "stationgroup": "synop_finland",
        "station": None,
        "run_interval": 600,
        "retry_count": 2,
        "retry_delay": 1,
        "verbose": False,
        "dry_run": False,
        "db_user": "user",
        "db_password": "pass",
        "db_host": "localhost",
        "db_name": "verifdb",
        "db_port": "5432",
    }
    defaults.update(kwargs)
    return Config(**defaults)  # type: ignore[arg-type]


def _cursor(rows: list[tuple[object, ...]] | None = None) -> MagicMock:
    cur = MagicMock()
    cur.fetchone.return_value = rows[0] if rows else None
    cur.fetchall.return_value = rows or []
    cur.mogrify.return_value = b"SQL"
    return cur


# ---------------------------------------------------------------------------
# load_config
# ---------------------------------------------------------------------------

_BASE_ENV = {
    "SMARTMET_SERVER_URL": "https://example.com/",
    "EDR_COLLECTION": "gfs",
    "VERIF_PRODUCER": "gfs",
    "SMARTMET_PARAMETERS": "Temperature",
    "SMARTMET_STATIONGROUP": "synop",
    "VERIFICATION_DB_USER": "u",
    "VERIFICATION_DB_PASSWORD": "p",
    "VERIFICATION_DB_HOST": "h",
    "VERIFICATION_DB_NAME": "db",
    "VERIFICATION_DB_PORT": "5432",
}


class TestLoadConfig:
    def test_valid_config(self) -> None:
        with patch.dict("os.environ", _BASE_ENV, clear=True):
            cfg = load_config()
        assert cfg.server_url == "https://example.com"  # trailing slash stripped
        assert cfg.edr_collection == "gfs"
        assert cfg.stationgroup == "synop"
        assert cfg.station is None
        assert cfg.run_interval == 600
        assert cfg.retry_count == 3
        assert cfg.retry_delay == 60
        assert cfg.verbose is False
        assert cfg.dry_run is False

    def test_station_instead_of_stationgroup(self) -> None:
        env = {**_BASE_ENV, "SMARTMET_STATION": "101004"}
        env.pop("SMARTMET_STATIONGROUP")
        with patch.dict("os.environ", env, clear=True):
            cfg = load_config()
        assert cfg.station == "101004"
        assert cfg.stationgroup is None

    def test_verbose_and_dry_run_flags(self) -> None:
        env = {**_BASE_ENV, "VERBOSE": "true", "DRY_RUN": "1"}
        with patch.dict("os.environ", env, clear=True):
            cfg = load_config()
        assert cfg.verbose is True
        assert cfg.dry_run is True

    def test_custom_intervals(self) -> None:
        env = {**_BASE_ENV, "RUN_INTERVAL": "300", "RETRY_COUNT": "5", "RETRY_DELAY": "30"}
        with patch.dict("os.environ", env, clear=True):
            cfg = load_config()
        assert cfg.run_interval == 300
        assert cfg.retry_count == 5
        assert cfg.retry_delay == 30

    def test_missing_required_exits(self) -> None:
        with patch.dict("os.environ", {}, clear=True), pytest.raises(SystemExit):
            load_config()

    def test_both_station_and_stationgroup_exits(self) -> None:
        env = {**_BASE_ENV, "SMARTMET_STATION": "101004"}
        with patch.dict("os.environ", env, clear=True), pytest.raises(SystemExit):
            load_config()

    def test_neither_station_nor_stationgroup_exits(self) -> None:
        env = {k: v for k, v in _BASE_ENV.items() if k != "SMARTMET_STATIONGROUP"}
        with patch.dict("os.environ", env, clear=True), pytest.raises(SystemExit):
            load_config()

    def test_invalid_run_interval_exits(self) -> None:
        env = {**_BASE_ENV, "RUN_INTERVAL": "abc"}
        with patch.dict("os.environ", env, clear=True), pytest.raises(SystemExit):
            load_config()

    def test_zero_run_interval_exits(self) -> None:
        env = {**_BASE_ENV, "RUN_INTERVAL": "0"}
        with patch.dict("os.environ", env, clear=True), pytest.raises(SystemExit):
            load_config()


# ---------------------------------------------------------------------------
# validate_params
# ---------------------------------------------------------------------------

class TestValidateParams:
    def test_known_param(self) -> None:
        cur = _cursor([(1,)])
        cfg = _cfg()
        with patch.object(cur, "fetchone", side_effect=[(1,), (3,)]):
            params = validate_params(cfg, cur)
        assert len(params) == 2
        assert params[0]["verif_name"] == "Temperature"
        assert params[0]["edr_name"] == "Temperature"

    def test_raw_suffix_stripped(self) -> None:
        cfg = _cfg(parameters="Temperature.raw")
        cur = _cursor([(1,)])
        params = validate_params(cfg, cur)
        assert params[0]["verif_name"] == "Temperature"
        assert params[0]["edr_name"] == "Temperature.raw"

    def test_unknown_param_warns_and_skips(self) -> None:
        cfg = _cfg(parameters="Bogus")
        cur = _cursor()
        with pytest.raises(RuntimeError, match="No valid parameters"):
            validate_params(cfg, cur)

    def test_dry_run_logs_query(self) -> None:
        cfg = _cfg(dry_run=True)
        cur = _cursor([(1,), (3,)])
        with patch.object(cur, "fetchone", side_effect=[(1,), (3,)]):
            validate_params(cfg, cur)
        assert cur.mogrify.called

    def test_verbose_logs_id(self) -> None:
        cfg = _cfg(verbose=True)
        cur = _cursor([(1,), (3,)])
        with patch.object(cur, "fetchone", side_effect=[(1,), (3,)]):
            params = validate_params(cfg, cur)
        assert len(params) == 2


# ---------------------------------------------------------------------------
# get_stations
# ---------------------------------------------------------------------------

class TestGetStations:
    def test_by_stationgroup(self) -> None:
        cfg = _cfg(stationgroup="synop_finland", station=None)
        rows = [(101001, "Helsinki", 25.0, 60.0)]
        cur = _cursor(rows)
        stations = get_stations(cfg, cur)
        assert stations == rows

    def test_by_station(self) -> None:
        cfg = _cfg(stationgroup=None, station="101001,101002")
        rows = [(101001, "Helsinki", 25.0, 60.0), (101002, "Tampere", 23.0, 61.0)]
        cur = _cursor(rows)
        stations = get_stations(cfg, cur)
        assert len(stations) == 2

    def test_no_stations_raises(self) -> None:
        cfg = _cfg()
        cur = _cursor([])
        with pytest.raises(RuntimeError, match="No stations found"):
            get_stations(cfg, cur)

    def test_dry_run_logs_query(self) -> None:
        cfg = _cfg(dry_run=True)
        rows = [(101001, "Helsinki", 25.0, 60.0)]
        cur = _cursor(rows)
        get_stations(cfg, cur)
        assert cur.mogrify.called


# ---------------------------------------------------------------------------
# get_producer_id / get_loaded_analysis_times
# ---------------------------------------------------------------------------

class TestGetProducerId:
    def test_found(self) -> None:
        cur = _cursor([(14,)])
        assert get_producer_id(cur, "gfs") == 14

    def test_not_found_raises(self) -> None:
        cur = _cursor()
        with pytest.raises(RuntimeError, match="not found in database"):
            get_producer_id(cur, "missing")


class TestGetLoadedAnalysisTimes:
    def test_returns_naive_datetimes(self) -> None:
        ts = datetime(2026, 5, 11, 0, 0, tzinfo=UTC)
        cur = _cursor([(ts,)])
        result = get_loaded_analysis_times(cur, 14)
        assert datetime(2026, 5, 11, 0, 0) in result

    def test_empty(self) -> None:
        cur = _cursor([])
        assert get_loaded_analysis_times(cur, 14) == set()


# ---------------------------------------------------------------------------
# get_instances
# ---------------------------------------------------------------------------

def _extent(start: str, end: str) -> dict[str, object]:
    return {"temporal": {"interval": [[start, end]]}}


class TestGetInstances:
    def _session(self, instances: list[dict[str, object]]) -> MagicMock:
        resp = MagicMock()
        resp.json.return_value = {"instances": instances}
        session = MagicMock()
        session.get.return_value = resp
        return session

    def test_basic(self) -> None:
        cfg = _cfg()
        inst = [{
            "id": "20260511T060000",
            "title": "",
            "extent": _extent("2026-05-11T06:00:00Z", "2026-05-21T06:00:00Z"),
        }]
        result = get_instances(cfg, self._session(inst))
        assert len(result) == 1
        assert result[0]["id"] == "20260511T060000"

    def test_sorted_oldest_first(self) -> None:
        cfg = _cfg()
        insts = [
            {
                "id": "20260511T060000",
                "title": "",
                "extent": _extent("2026-05-11T06:00:00Z", "2026-05-21T06:00:00Z"),
            },
            {
                "id": "20260510T060000",
                "title": "",
                "extent": _extent("2026-05-10T06:00:00Z", "2026-05-20T06:00:00Z"),
            },
        ]
        result = get_instances(cfg, self._session(insts))
        assert result[0]["id"] == "20260510T060000"
        assert result[1]["id"] == "20260511T060000"

    def test_with_title_expected_steps(self) -> None:
        cfg = _cfg()
        title = "Starttime: 2026-05-11T00:00:00Z Endtime: 2026-05-11T06:00:00Z Timestep: 60"
        inst = [{
            "id": "20260511T000000",
            "title": title,
            "extent": _extent("2026-05-11T00:00:00Z", "2026-05-11T06:00:00Z"),
        }]
        result = get_instances(cfg, self._session(inst))
        assert result[0]["expected_steps"] == 7

    def test_http_error_raises(self) -> None:
        cfg = _cfg()
        resp = MagicMock()
        resp.raise_for_status.side_effect = requests.HTTPError("500")
        session = MagicMock()
        session.get.return_value = resp
        with pytest.raises(requests.HTTPError):
            get_instances(cfg, session)


# ---------------------------------------------------------------------------
# fetch_instance_data
# ---------------------------------------------------------------------------

def _covjson() -> dict[str, object]:
    return {
        "domain": {"axes": {"t": {"values": ["2026-05-11T00:00:00Z"]}}},
        "ranges": {"temperature": {"values": [15.0]}},
    }


_INSTANCE: dict[str, object] = {
    "id": "20260511T000000",
    "start": "2026-05-11T00:00:00Z",
    "end": "2026-05-21T00:00:00Z",
}
_PARAMS: list[dict[str, object]] = [
    {"verif_name": "Temperature", "verif_id": 1, "edr_name": "Temperature"}
]
_STATIONS: list[tuple[object, ...]] = [(101001, "Helsinki", 25.0, 60.0)]


class TestFetchInstanceData:
    def test_successful_fetch(self) -> None:
        cfg = _cfg()
        resp = MagicMock()
        resp.ok = True
        resp.json.return_value = _covjson()
        session = MagicMock()
        session.get.return_value = resp
        result = fetch_instance_data(cfg, session, _INSTANCE, _STATIONS, _PARAMS)
        assert 101001 in result

    def test_400_skips_station(self) -> None:
        cfg = _cfg()
        resp = MagicMock()
        resp.ok = False
        resp.status_code = 400
        session = MagicMock()
        session.get.return_value = resp
        result = fetch_instance_data(cfg, session, _INSTANCE, _STATIONS, _PARAMS)
        assert result == {}

    def test_non_400_error_raises(self) -> None:
        cfg = _cfg()
        resp = MagicMock()
        resp.ok = False
        resp.status_code = 503
        session = MagicMock()
        session.get.return_value = resp
        with pytest.raises(RuntimeError, match="HTTP 503"):
            fetch_instance_data(cfg, session, _INSTANCE, _STATIONS, _PARAMS)

    def test_dry_run_skips_request(self) -> None:
        cfg = _cfg(dry_run=True)
        session = MagicMock()
        result = fetch_instance_data(cfg, session, _INSTANCE, _STATIONS, _PARAMS)
        session.get.assert_not_called()
        assert result == {}

    def test_verbose_logs_station(self) -> None:
        cfg = _cfg(verbose=True)
        resp = MagicMock()
        resp.ok = True
        resp.json.return_value = _covjson()
        session = MagicMock()
        session.get.return_value = resp
        result = fetch_instance_data(cfg, session, _INSTANCE, _STATIONS, _PARAMS)
        assert 101001 in result


# ---------------------------------------------------------------------------
# fetch_with_retry
# ---------------------------------------------------------------------------

def _complete_data() -> dict[object, object]:
    return {
        101001: {
            "domain": {"axes": {"t": {"values": [
                "2026-05-11T00:00:00Z", "2026-05-11T01:00:00Z",
            ]}}},
            "ranges": {},
        }
    }


class TestFetchWithRetry:
    def test_success_on_first_attempt(self) -> None:
        cfg = _cfg(retry_count=2)
        session = MagicMock()
        with patch(
            "smartmet_verify_model_data_loader._core.fetch_instance_data",
            return_value=_complete_data(),
        ):
            result = fetch_with_retry(cfg, session, _INSTANCE, _STATIONS, _PARAMS)
        assert 101001 in result

    def test_retries_on_exception(self) -> None:
        cfg = _cfg(retry_count=2, retry_delay=0)
        session = MagicMock()
        side_effects = [RuntimeError("fail"), RuntimeError("fail"), _complete_data()]
        with (
            patch(
                "smartmet_verify_model_data_loader._core.fetch_instance_data",
                side_effect=side_effects,
            ),
            patch("smartmet_verify_model_data_loader._core._stop") as mock_stop,
        ):
            mock_stop.wait.return_value = False
            result = fetch_with_retry(cfg, session, _INSTANCE, _STATIONS, _PARAMS)
        assert 101001 in result

    def test_raises_after_all_retries_exhausted(self) -> None:
        cfg = _cfg(retry_count=1, retry_delay=0)
        session = MagicMock()
        with (
            patch(
                "smartmet_verify_model_data_loader._core.fetch_instance_data",
                side_effect=RuntimeError("persistent fail"),
            ),
            patch("smartmet_verify_model_data_loader._core._stop") as mock_stop,
        ):
            mock_stop.wait.return_value = False
            with pytest.raises(RuntimeError, match="persistent fail"):
                fetch_with_retry(cfg, session, _INSTANCE, _STATIONS, _PARAMS)

    def test_stop_event_breaks_retry_loop(self) -> None:
        cfg = _cfg(retry_count=3, retry_delay=1)
        session = MagicMock()
        with (
            patch(
                "smartmet_verify_model_data_loader._core.fetch_instance_data",
                side_effect=RuntimeError("fail"),
            ),
            patch("smartmet_verify_model_data_loader._core._stop") as mock_stop,
        ):
            mock_stop.wait.return_value = True  # stop requested
            with pytest.raises(RuntimeError):
                fetch_with_retry(cfg, session, _INSTANCE, _STATIONS, _PARAMS)


# ---------------------------------------------------------------------------
# load_to_db
# ---------------------------------------------------------------------------

def _rows() -> list[str]:
    return ["14\t101001\t2026-05-11 00:00:00\t1\t\\N\t0\t15.0"]


class TestLoadToDb:
    def test_copy_success(self) -> None:
        cur = MagicMock()
        load_to_db(cur, 14, "gfs", datetime(2026, 5, 11), _rows())
        cur.copy_from.assert_called_once()
        assert cur.execute.call_count == 1  # forecasts INSERT

    def test_copy_duplicate_key_falls_back_to_upsert(self) -> None:
        class _DupKeyError(Exception):
            pgcode = "23505"

        cur = MagicMock()
        cur.copy_from.side_effect = [_DupKeyError(), None]
        with patch("smartmet_verify_model_data_loader._core.psycopg2.IntegrityError", _DupKeyError):
            load_to_db(cur, 14, "gfs", datetime(2026, 5, 11), _rows())
        assert cur.copy_from.call_count == 2  # first attempt + temp_load
        assert cur.execute.call_count == 3  # forecasts INSERT + CREATE TEMP + INSERT SELECT

    def test_non_duplicate_integrity_error_reraises(self) -> None:
        class _FKError(Exception):
            pgcode = "23503"

        cur = MagicMock()
        cur.copy_from.side_effect = _FKError()
        with (
            patch("smartmet_verify_model_data_loader._core.psycopg2.IntegrityError", _FKError),
            pytest.raises(_FKError),
        ):
            load_to_db(cur, 14, "gfs", datetime(2026, 5, 11), _rows())
