"""Tests for smartmet_verify_model_data_loader._core."""

from datetime import datetime

import pytest

from smartmet_verify_model_data_loader._core import (
    _check_completeness,
    _parse_expected_steps,
    build_copy_buffer,
    parse_instance_id,
)


class TestParseExpectedSteps:
    def test_valid_hourly(self) -> None:
        title = "Starttime: 2026-05-11T00:00:00Z Endtime: 2026-05-11T06:00:00Z Timestep: 60"
        # 6 hours / 60 min + 1 = 7
        assert _parse_expected_steps(title) == 7

    def test_valid_6h_timestep(self) -> None:
        title = "Starttime: 2026-05-11T00:00:00Z Endtime: 2026-05-11T18:00:00Z Timestep: 360"
        # 18 hours / 6 hours + 1 = 4
        assert _parse_expected_steps(title) == 4

    def test_embedded_in_longer_string(self) -> None:
        title = (
            "GFS Starttime: 2026-05-11T00:00:00Z Endtime: 2026-05-11T03:00:00Z Timestep: 60 data"
        )
        assert _parse_expected_steps(title) == 4

    def test_no_match(self) -> None:
        assert _parse_expected_steps("no timing info here") is None

    def test_empty_string(self) -> None:
        assert _parse_expected_steps("") is None

    def test_zero_timestep(self) -> None:
        title = "Starttime: 2026-05-11T00:00:00Z Endtime: 2026-05-21T00:00:00Z Timestep: 0"
        assert _parse_expected_steps(title) is None

    def test_invalid_start_date(self) -> None:
        title = "Starttime: not-a-date Endtime: 2026-05-21T00:00:00Z Timestep: 60"
        assert _parse_expected_steps(title) is None


class TestParseInstanceId:
    def test_midnight(self) -> None:
        assert parse_instance_id("20260511T000000") == datetime(2026, 5, 11, 0, 0, 0)

    def test_non_zero_hour(self) -> None:
        assert parse_instance_id("20260510T060000") == datetime(2026, 5, 10, 6, 0, 0)

    def test_invalid_raises(self) -> None:
        with pytest.raises(ValueError):
            parse_instance_id("invalid")


class TestCheckCompleteness:
    def _station_data(self, timestamps: list[str]) -> dict[object, object]:
        return {
            101001: {
                "domain": {"axes": {"t": {"values": timestamps}}},
                "ranges": {},
            }
        }

    def test_complete_with_expected(self) -> None:
        instance: dict[str, object] = {"expected_steps": 3}
        data = self._station_data([
            "2026-05-11T00:00:00Z", "2026-05-11T01:00:00Z", "2026-05-11T02:00:00Z",
        ])
        assert _check_completeness(instance, data) is None

    def test_short_with_expected(self) -> None:
        instance: dict[str, object] = {"expected_steps": 5}
        data = self._station_data(["2026-05-11T00:00:00Z", "2026-05-11T01:00:00Z"])
        result = _check_completeness(instance, data)
        assert result is not None
        assert "2/5" in result

    def test_no_gaps_without_expected(self) -> None:
        instance: dict[str, object] = {"expected_steps": None}
        data = self._station_data([
            "2026-05-11T00:00:00Z",
            "2026-05-11T01:00:00Z",
            "2026-05-11T02:00:00Z",
        ])
        assert _check_completeness(instance, data) is None

    def test_gap_detected_without_expected(self) -> None:
        instance: dict[str, object] = {"expected_steps": None}
        data = self._station_data([
            "2026-05-11T00:00:00Z",
            "2026-05-11T01:00:00Z",
            "2026-05-11T03:00:00Z",  # 2h gap after 1h intervals
        ])
        result = _check_completeness(instance, data)
        assert result is not None
        assert "gaps" in result

    def test_single_step_no_gap_check(self) -> None:
        instance: dict[str, object] = {"expected_steps": None}
        data = self._station_data(["2026-05-11T00:00:00Z"])
        assert _check_completeness(instance, data) is None

    def test_empty_data_returns_none(self) -> None:
        assert _check_completeness({"expected_steps": 3}, {}) is None


class TestBuildCopyBuffer:
    def _covjson(self, timestamps: list[str], values: list[object]) -> dict[str, object]:
        return {
            "domain": {"axes": {"t": {"values": timestamps}}},
            "ranges": {"temperature": {"values": values}},
        }

    def test_basic_rows_and_analysis_time(self) -> None:
        params: list[dict[str, object]] = [
            {"verif_name": "Temperature", "verif_id": 1, "edr_name": "Temperature"}
        ]
        instance: dict[str, object] = {"id": "20260511T000000"}
        data: dict[object, object] = {
            101001: self._covjson(
                ["2026-05-11T00:00:00Z", "2026-05-11T01:00:00Z", "2026-05-11T06:00:00Z"],
                [15.0, 14.0, 13.0],
            )
        }
        rows, analysis_time = build_copy_buffer(14, params, instance, data)
        assert analysis_time == datetime(2026, 5, 11, 0, 0, 0)
        assert len(rows) == 3
        # leadtime 0 for first step
        fields = rows[0].split("\t")
        assert fields[0] == "14"       # producer_id
        assert fields[1] == "101001"   # fmisid
        assert fields[5] == "0"        # leadtime
        assert fields[6] == "15.0"     # value
        # leadtime 6 for last step
        assert rows[2].split("\t")[5] == "6"

    def test_none_values_skipped(self) -> None:
        params: list[dict[str, object]] = [
            {"verif_name": "Temperature", "verif_id": 1, "edr_name": "Temperature"}
        ]
        instance: dict[str, object] = {"id": "20260511T000000"}
        data: dict[object, object] = {
            101001: self._covjson(
                ["2026-05-11T00:00:00Z", "2026-05-11T01:00:00Z"],
                [15.0, None],
            )
        }
        rows, _ = build_copy_buffer(14, params, instance, data)
        assert len(rows) == 1

    def test_missing_parameter_key_skipped(self) -> None:
        params: list[dict[str, object]] = [
            {"verif_name": "WindSpeedMS", "verif_id": 3, "edr_name": "WindSpeedMS"}
        ]
        instance: dict[str, object] = {"id": "20260511T000000"}
        data: dict[object, object] = {
            101001: self._covjson(["2026-05-11T00:00:00Z"], [15.0])
            # ranges only has "temperature", not "windspeedms"
        }
        rows, _ = build_copy_buffer(14, params, instance, data)
        assert len(rows) == 0

    def test_multiple_stations(self) -> None:
        params: list[dict[str, object]] = [
            {"verif_name": "Temperature", "verif_id": 1, "edr_name": "Temperature"}
        ]
        instance: dict[str, object] = {"id": "20260511T000000"}
        data: dict[object, object] = {
            101001: self._covjson(["2026-05-11T00:00:00Z"], [15.0]),
            101002: self._covjson(["2026-05-11T00:00:00Z"], [20.0]),
        }
        rows, _ = build_copy_buffer(14, params, instance, data)
        assert len(rows) == 2

    def test_multiple_params(self) -> None:
        params: list[dict[str, object]] = [
            {"verif_name": "Temperature", "verif_id": 1, "edr_name": "Temperature"},
            {"verif_name": "WindSpeedMS", "verif_id": 3, "edr_name": "WindSpeedMS"},
        ]
        instance: dict[str, object] = {"id": "20260511T000000"}
        covjson: dict[str, object] = {
            "domain": {"axes": {"t": {"values": ["2026-05-11T00:00:00Z"]}}},
            "ranges": {
                "temperature": {"values": [15.0]},
                "windspeedms": {"values": [5.0]},
            },
        }
        data: dict[object, object] = {101001: covjson}
        rows, _ = build_copy_buffer(14, params, instance, data)
        assert len(rows) == 2

    def test_raw_param_suffix_lowercased(self) -> None:
        """edr_name ending in .raw should be lowercased in the key lookup."""
        params: list[dict[str, object]] = [
            {"verif_name": "Temperature", "verif_id": 1, "edr_name": "Temperature.raw"}
        ]
        instance: dict[str, object] = {"id": "20260511T000000"}
        data: dict[object, object] = {
            101001: {
                "domain": {"axes": {"t": {"values": ["2026-05-11T00:00:00Z"]}}},
                "ranges": {"temperature.raw": {"values": [15.0]}},
            }
        }
        rows, _ = build_copy_buffer(14, params, instance, data)
        assert len(rows) == 1
