"""Test suite.

Run with ``pytest -v`` from the project root.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from pipeline.cleaning import (
    apply_string_transforms,
    clean_dataset,
    coerce_series,
    is_null,
    normalize_nulls,
)
from pipeline.config import (
    ConfigError,
    FieldSpec,
    SchemaSpec,
    SourceSpec,
    apply_env_overrides,
    load_config,
)
from pipeline.pipeline import run_pipeline
from pipeline.readers import ReaderError, normalize_column_name, read_csv, read_json
from pipeline.transform import TransformError, apply_transforms
from pipeline.writers import write_dataframe

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def field(name: str, **kwargs) -> FieldSpec:
    return FieldSpec(name=name, **kwargs)


def schema(*fields: FieldSpec, **kwargs) -> SchemaSpec:
    return SchemaSpec(fields=list(fields), **kwargs)


# ---------------------------------------------------------------------------
# config
# ---------------------------------------------------------------------------

class TestConfig:
    def test_loads_the_bundled_config(self):
        config = load_config(PROJECT_ROOT / "config.yaml")
        assert config.run.name == "customer-orders"
        assert {s.name for s in config.sources} == {"customers", "orders"}
        assert config.outputs

    def test_missing_file_is_a_clear_error(self):
        with pytest.raises(ConfigError, match="not found"):
            load_config("/nonexistent/config.yaml")

    def test_rejects_unknown_field_type(self):
        with pytest.raises(ConfigError, match="unknown type"):
            FieldSpec(name="x", type="complex_number")

    def test_rejects_fill_default_without_a_default(self):
        with pytest.raises(ConfigError, match="requires a 'default'"):
            FieldSpec(name="x", fill="default")

    def test_rejects_mean_fill_on_a_string_column(self):
        with pytest.raises(ConfigError, match="numeric types"):
            FieldSpec(name="x", type="string", fill="mean")

    def test_rejects_duplicate_field_names(self):
        with pytest.raises(ConfigError, match="duplicate field names"):
            schema(field("a"), field("a"))

    def test_rejects_dedupe_on_an_undeclared_field(self):
        with pytest.raises(ConfigError, match="undeclared fields"):
            schema(field("a"), dedupe_on=["b"])

    def test_api_source_requires_a_url(self):
        with pytest.raises(ConfigError, match="requires 'url'"):
            SourceSpec(name="s", type="api", schema=schema(field("a")))

    def test_env_overrides_are_applied_and_typed(self):
        data = {"logging": {"level": "INFO"}, "pipeline": {"fail_fast": False}}
        applied = apply_env_overrides(
            data,
            {
                "PIPELINE__LOGGING__LEVEL": "DEBUG",
                "PIPELINE__PIPELINE__FAIL_FAST": "true",
                "UNRELATED": "ignored",
            },
        )
        assert data["logging"]["level"] == "DEBUG"
        assert data["pipeline"]["fail_fast"] is True
        assert len(applied) == 2


# ---------------------------------------------------------------------------
# readers
# ---------------------------------------------------------------------------

class TestReaders:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("  Customer ID ", "customer_id"),
            ("Total (£)", "total"),
            ("already_fine", "already_fine"),
            ("", "unnamed"),
            (123, "123"),
        ],
    )
    def test_header_normalisation(self, raw, expected):
        assert normalize_column_name(raw) == expected

    def test_csv_with_a_byte_order_mark(self, tmp_path):
        path = tmp_path / "bom.csv"
        path.write_bytes("\ufeffCustomer ID,Name\nC1,Ana\n".encode("utf-8"))
        spec = SourceSpec(name="s", type="csv", path=str(path), schema=schema(field("a")))
        frame = read_csv(spec, tmp_path)
        assert list(frame.columns) == ["customer_id", "name"]

    def test_empty_csv_returns_an_empty_frame(self, tmp_path):
        path = tmp_path / "empty.csv"
        path.write_text("")
        spec = SourceSpec(name="s", type="csv", path=str(path), schema=schema(field("a")))
        assert read_csv(spec, tmp_path).empty

    def test_missing_file_raises(self, tmp_path):
        spec = SourceSpec(name="s", type="csv", path="nope.csv", schema=schema(field("a")))
        with pytest.raises(ReaderError, match="file not found"):
            read_csv(spec, tmp_path)

    def test_duplicate_headers_are_renamed(self, tmp_path):
        path = tmp_path / "dupe.csv"
        path.write_text("id,id\n1,2\n")
        spec = SourceSpec(name="s", type="csv", path=str(path), schema=schema(field("a")))
        assert len(set(read_csv(spec, tmp_path).columns)) == 2

    def test_nested_json_is_flattened(self, tmp_path):
        path = tmp_path / "n.json"
        path.write_text(json.dumps([{"id": 1, "ship": {"country": "GB"}}]))
        spec = SourceSpec(name="s", type="json", path=str(path), schema=schema(field("a")))
        frame = read_json(spec, tmp_path)
        assert "ship_country" in frame.columns

    def test_newline_delimited_json_falls_back_cleanly(self, tmp_path):
        path = tmp_path / "n.jsonl"
        path.write_text('{"id": 1}\n{"id": 2}\n')
        spec = SourceSpec(name="s", type="json", path=str(path), schema=schema(field("a")))
        assert len(read_json(spec, tmp_path)) == 2

    def test_record_path_drills_into_the_payload(self, tmp_path):
        path = tmp_path / "w.json"
        path.write_text(json.dumps({"payload": {"rows": [{"id": 1}, {"id": 2}]}}))
        spec = SourceSpec(
            name="s", type="json", path=str(path),
            options={"record_path": "payload.rows"}, schema=schema(field("a")),
        )
        assert len(read_json(spec, tmp_path)) == 2

    def test_malformed_json_raises_a_useful_error(self, tmp_path):
        path = tmp_path / "bad.json"
        path.write_text("{this is not json")
        spec = SourceSpec(name="s", type="json", path=str(path), schema=schema(field("a")))
        with pytest.raises(ReaderError, match="invalid JSON"):
            read_json(spec, tmp_path)


# ---------------------------------------------------------------------------
# cleaning
# ---------------------------------------------------------------------------

class TestNulls:
    @pytest.mark.parametrize("value", [None, float("nan"), pd.NaT, "", "   "])
    def test_recognises_every_flavour_of_missing(self, value):
        assert is_null(value)

    @pytest.mark.parametrize("value", ["a", 0, False, 0.0])
    def test_does_not_treat_falsy_values_as_missing(self, value):
        assert not is_null(value)

    def test_configured_tokens_become_null(self):
        series = pd.Series(["N/A", "value", "-", ""])
        out = normalize_nulls(series, ["n/a", "-", ""])
        assert out.isna().tolist() == [True, False, True, True]

    def test_nulls_never_become_the_string_nan(self):
        """Regression: pandas turns a mapped None into NaN, and str(nan) == 'nan'."""
        series = normalize_nulls(pd.Series(["N/A", "ok"]), ["n/a"])
        out = apply_string_transforms(series, ["strip", "title"])
        assert out.iloc[0] is None or pd.isna(out.iloc[0])
        assert "nan" not in [str(v).lower() for v in out.dropna()]


class TestCoercion:
    def test_integers_tolerate_separators(self):
        out, failed = coerce_series(pd.Series(["1,200", "42", "7.0"]), field("n", type="integer"))
        assert out.tolist() == [1200, 42, 7]
        assert not failed.any()

    def test_non_integral_values_fail_rather_than_silently_rounding(self):
        out, failed = coerce_series(pd.Series(["3.5"]), field("n", type="integer"))
        assert failed.iloc[0] and pd.isna(out.iloc[0])

    def test_currency_symbols_are_stripped_for_floats(self):
        spec = field("amount", type="float", transform=["strip_currency"])
        series = apply_string_transforms(pd.Series(["£1,200.50", "$850.00"]), spec.transform)
        out, failed = coerce_series(series, spec)
        assert out.tolist() == [1200.50, 850.00]
        assert not failed.any()

    def test_accounting_negatives_are_understood(self):
        out, _ = coerce_series(pd.Series(["(500)"]), field("amount", type="float"))
        assert out.iloc[0] == -500.0

    @pytest.mark.parametrize(
        "raw,expected",
        [("yes", True), ("TRUE", True), ("1", True), ("no", False), ("0", False), ("N", False)],
    )
    def test_boolean_tokens(self, raw, expected):
        out, failed = coerce_series(pd.Series([raw]), field("flag", type="boolean"))
        assert out.iloc[0] == expected and not failed.iloc[0]

    def test_unparseable_boolean_is_flagged(self):
        out, failed = coerce_series(pd.Series(["maybe"]), field("flag", type="boolean"))
        assert failed.iloc[0] and pd.isna(out.iloc[0])

    def test_mixed_date_formats_all_parse(self):
        series = pd.Series(["2023-01-15", "14/02/2023", "03 Apr 2023", "2023/05/20"])
        out, failed = coerce_series(series, field("d", type="date"))
        assert not failed.any()
        assert out.dt.strftime("%Y-%m-%d").tolist() == [
            "2023-01-15", "2023-02-14", "2023-04-03", "2023-05-20",
        ]

    def test_garbage_dates_are_flagged_not_guessed(self):
        out, failed = coerce_series(pd.Series(["not a date"]), field("d", type="date"))
        assert failed.iloc[0] and pd.isna(out.iloc[0])


class TestCleanDataset:
    def test_missing_required_value_rejects_the_row(self):
        frame = pd.DataFrame({"id": ["A", None], "name": ["Ana", "Ben"]})
        result = clean_dataset(frame, schema(field("id", required=True), field("name")), "t")
        assert result.stats.rows_out == 1
        assert result.stats.rows_rejected == 1
        assert "required" in result.rejects["_reject_reason"].iloc[0]

    def test_whitespace_only_required_value_is_rejected(self):
        frame = pd.DataFrame({"name": ["   ", "Ben"]})
        result = clean_dataset(
            frame, schema(field("name", required=True, transform=["strip"])), "t"
        )
        assert result.stats.rows_out == 1

    def test_values_outside_the_allowed_set_are_rejected(self):
        frame = pd.DataFrame({"country": ["GB", "XX"]})
        result = clean_dataset(frame, schema(field("country", allowed=["GB", "US"])), "t")
        assert result.stats.rows_out == 1
        assert result.stats.fields["country"].not_allowed == 1

    def test_out_of_range_numbers_are_rejected(self):
        frame = pd.DataFrame({"age": ["34", "250"]})
        result = clean_dataset(frame, schema(field("age", type="integer", max=120)), "t")
        assert result.stats.rows_out == 1

    def test_median_fill_replaces_missing_numbers(self):
        frame = pd.DataFrame({"n": ["10", "20", "30", None]})
        result = clean_dataset(frame, schema(field("n", type="integer", fill="median")), "t")
        assert result.frame["n"].isna().sum() == 0
        assert result.stats.fields["n"].filled == 1

    def test_default_fill_uses_the_declared_value(self):
        frame = pd.DataFrame({"flag": ["yes", None]})
        result = clean_dataset(
            frame, schema(field("flag", type="boolean", fill="default", default=False)), "t"
        )
        assert result.frame["flag"].tolist() == [True, False]

    def test_duplicates_are_removed_on_the_declared_key(self):
        frame = pd.DataFrame({"id": ["A", "A", "B"]})
        result = clean_dataset(frame, schema(field("id"), dedupe_on=["id"]), "t")
        assert result.stats.duplicates_removed == 1
        assert result.stats.rows_out == 2

    def test_an_absent_column_becomes_nulls_rather_than_crashing(self):
        frame = pd.DataFrame({"a": ["1"]})
        result = clean_dataset(frame, schema(field("a"), field("b")), "t")
        assert "b" in result.frame.columns
        assert result.stats.columns_missing == ["b"]

    def test_undeclared_columns_are_dropped_by_default(self):
        frame = pd.DataFrame({"a": ["1"], "junk": ["x"]})
        result = clean_dataset(frame, schema(field("a")), "t")
        assert list(result.frame.columns) == ["a"]

    def test_empty_input_produces_an_empty_but_shaped_frame(self):
        result = clean_dataset(pd.DataFrame(), schema(field("a"), field("b")), "t")
        assert result.frame.empty
        assert list(result.frame.columns) == ["a", "b"]

    def test_source_name_maps_an_input_column_to_a_new_name(self):
        frame = pd.DataFrame({"shipping_country": ["GB"]})
        result = clean_dataset(
            frame, schema(field("ship_country", source_name="shipping_country")), "t"
        )
        assert result.frame["ship_country"].iloc[0] == "GB"


# ---------------------------------------------------------------------------
# transforms
# ---------------------------------------------------------------------------

class TestTransforms:
    @pytest.fixture
    def datasets(self):
        return {
            "left": pd.DataFrame({"id": ["A", "B"], "name": ["Ana", "Ben"]}),
            "right": pd.DataFrame({"id": ["A", "A", "B"], "qty": [2, 3, 1], "price": [10.0, 5.0, 8.0]}),
        }

    def test_join(self, datasets):
        out = apply_transforms(
            datasets,
            [{"type": "join", "left": "left", "right": "right", "left_on": "id",
              "how": "inner", "name": "joined"}],
        )
        assert len(out["joined"]) == 3

    def test_arithmetic_derivation(self, datasets):
        out = apply_transforms(
            datasets,
            [{"type": "derive_arithmetic", "dataset": "right", "target": "total",
              "left": "qty", "op": "*", "right": "price"}],
        )
        assert out["right"]["total"].tolist() == [20.0, 15.0, 8.0]

    def test_division_by_zero_becomes_null_not_an_exception(self):
        datasets = {"d": pd.DataFrame({"a": [10.0], "b": [0.0]})}
        out = apply_transforms(
            datasets,
            [{"type": "derive_arithmetic", "dataset": "d", "target": "r",
              "left": "a", "op": "/", "right": "b"}],
        )
        assert pd.isna(out["d"]["r"].iloc[0])

    def test_aggregate(self, datasets):
        out = apply_transforms(
            datasets,
            [{"type": "aggregate", "dataset": "right", "name": "agg", "group_by": ["id"],
              "aggregations": [{"column": "qty", "func": "sum", "target": "total_qty"}]}],
        )
        assert sorted(out["agg"]["total_qty"].tolist()) == [1, 5]

    def test_filter_rows(self, datasets):
        out = apply_transforms(
            datasets,
            [{"type": "filter_rows", "dataset": "right", "column": "qty", "op": ">=",
              "value": 2, "name": "big"}],
        )
        assert len(out["big"]) == 2

    def test_map_values(self, datasets):
        out = apply_transforms(
            {"d": pd.DataFrame({"m": ["express", "standard", "other"]})},
            [{"type": "map_values", "dataset": "d", "source": "m", "target": "tier",
              "mapping": {"express": "priority"}, "default": "economy"}],
        )
        assert out["d"]["tier"].tolist() == ["priority", "economy", "economy"]

    def test_unknown_transform_type_is_rejected(self, datasets):
        with pytest.raises(TransformError, match="unknown type"):
            apply_transforms(datasets, [{"type": "teleport"}])

    def test_referring_to_a_missing_dataset_is_rejected(self, datasets):
        with pytest.raises(TransformError, match="no dataset named"):
            apply_transforms(datasets, [{"type": "filter_rows", "dataset": "ghost",
                                         "column": "a", "op": "=="}])

    def test_referring_to_a_missing_column_is_rejected(self, datasets):
        with pytest.raises(TransformError, match="no column"):
            apply_transforms(datasets, [{"type": "filter_rows", "dataset": "right",
                                         "column": "ghost", "op": "==", "value": 1}])


# ---------------------------------------------------------------------------
# writers
# ---------------------------------------------------------------------------

class TestWriters:
    @pytest.fixture
    def frame(self):
        return pd.DataFrame({"a": [1, 2], "b": ["x", None]})

    def test_csv_round_trip(self, frame, tmp_path):
        path = write_dataframe(frame, tmp_path / "out.csv", "csv")
        assert len(pd.read_csv(path)) == 2

    def test_json_output_is_valid(self, frame, tmp_path):
        path = write_dataframe(frame, tmp_path / "out.json", "json")
        assert len(json.loads(path.read_text())) == 2

    def test_jsonl_writes_one_object_per_line(self, frame, tmp_path):
        path = write_dataframe(frame, tmp_path / "out.jsonl", "jsonl")
        lines = [l for l in path.read_text().splitlines() if l]
        assert len(lines) == 2 and all(json.loads(l) for l in lines)

    def test_dates_serialise_to_iso_strings(self, tmp_path):
        frame = pd.DataFrame({"d": pd.to_datetime(["2024-01-15"])})
        path = write_dataframe(frame, tmp_path / "d.json", "json")
        assert "2024-01-15" in json.loads(path.read_text())[0]["d"]

    def test_no_temporary_files_are_left_behind(self, frame, tmp_path):
        write_dataframe(frame, tmp_path / "out.csv", "csv")
        assert not list(tmp_path.glob("*.tmp"))


# ---------------------------------------------------------------------------
# end to end
# ---------------------------------------------------------------------------

class TestEndToEnd:
    def test_bundled_config_runs_and_produces_every_output(self, tmp_path):
        config = load_config(PROJECT_ROOT / "config.yaml")
        config.run.output_dir = str(tmp_path)
        report = run_pipeline(config)

        assert report.status == "completed"
        assert not report.errors
        for spec in config.outputs:
            assert (tmp_path / spec.filename).exists()
        assert (tmp_path / "run_report.json").exists()
        assert (tmp_path / "rejected_rows.csv").exists()

    def test_clean_output_contains_no_literal_nan_text(self, tmp_path):
        config = load_config(PROJECT_ROOT / "config.yaml")
        config.run.output_dir = str(tmp_path)
        run_pipeline(config)
        text = (tmp_path / "customers_clean.csv").read_text()
        assert ",nan," not in text.lower()

    def test_dry_run_writes_nothing(self, tmp_path):
        config = load_config(PROJECT_ROOT / "config.yaml")
        config.run.output_dir = str(tmp_path)
        report = run_pipeline(config, dry_run=True)
        assert report.status == "dry-run"
        assert not list(tmp_path.iterdir())

    def test_report_records_per_source_statistics(self, tmp_path):
        config = load_config(PROJECT_ROOT / "config.yaml")
        config.run.output_dir = str(tmp_path)
        report = run_pipeline(config)
        customers = report.sources["customers"]
        assert customers["rows_in"] > customers["rows_out"]
        assert customers["duplicates_removed"] == 1
