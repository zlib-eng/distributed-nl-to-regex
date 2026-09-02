"""
Tests for the transformations/ package: the strategy + registry
pattern the README's "Open/Closed principle" claim rests on.

Three layers get exercised here:

1. registry.py — does asking for a known type return the right class,
   and does asking for an unknown type fail loudly instead of silently?
2. generate_spec() on each strategy — is it actually calling the right
   llm_utils function for its type? (mocked, no real LLM/network calls)
3. apply() on each strategy — does it produce the correct output on a
   REAL (local, in-process) Spark DataFrame? This is the part a
   pure-mock test suite would never catch: e.g. an off-by-one in a
   regexp_extract group index, or the wrong Spark function wired to
   the wrong format string.

A single shared local SparkSession is created once for the whole test
class (session creation is the slow part, ~5-10s) and reused across
tests, then stopped in tearDownClass.
"""
from unittest.mock import patch

from django.test import TestCase, SimpleTestCase

from apps.jobs.transformations.registry import (
    get_transformation,
    TRANSFORMATION_REGISTRY,
)
from apps.jobs.transformations.find_replace import FindReplaceTransformation
from apps.jobs.transformations.extract import ExtractTransformation
from apps.jobs.transformations.standardize_format import (
    StandardizeFormatTransformation,
)


class RegistryTests(SimpleTestCase):

    def test_known_types_resolve_to_correct_classes(self):
        self.assertIsInstance(
            get_transformation("FIND_REPLACE"), FindReplaceTransformation
        )
        self.assertIsInstance(
            get_transformation("EXTRACT"), ExtractTransformation
        )
        self.assertIsInstance(
            get_transformation("STANDARDIZE_FORMAT"),
            StandardizeFormatTransformation,
        )

    def test_unknown_type_raises_value_error(self):
        # This is what protects tasks.py: a typo'd or future-removed
        # transformation_type must fail loudly and early, not silently
        # do nothing or crash somewhere deep inside Spark.
        with self.assertRaises(ValueError):
            get_transformation("DOES_NOT_EXIST")

    def test_every_registry_entry_implements_the_transformation_interface(self):
        # Guards against a future strategy class being registered
        # without actually implementing generate_spec/apply — this
        # would currently only surface at runtime, on whatever request
        # happens to hit that type first.
        for name, cls in TRANSFORMATION_REGISTRY.items():
            with self.subTest(transformation_type=name):
                instance = cls()
                self.assertTrue(hasattr(instance, "generate_spec"))
                self.assertTrue(hasattr(instance, "apply"))

    def test_registry_returns_a_fresh_instance_each_call(self):
        # generate_spec/apply carry no state between calls by design;
        # confirming a new instance is handed out each time protects
        # against a future accidental shared-instance bug (e.g. someone
        # "optimizing" the registry to cache instances and introducing
        # cross-job state leakage).
        first = get_transformation("FIND_REPLACE")
        second = get_transformation("FIND_REPLACE")
        self.assertIsNot(first, second)


class GenerateSpecWiringTests(SimpleTestCase):
    """
    Confirms each strategy's generate_spec() calls the LLM path its
    type is supposed to use, and shapes the result into the dict
    apply() expects. llm_utils itself is mocked — this is purely
    about whether the wiring between the two files is correct.
    """

    @patch("apps.jobs.transformations.find_replace.get_regex_pattern")
    def test_find_replace_uses_regex_path(self, mock_get_pattern):
        mock_get_pattern.return_value = r"\d+"

        spec = FindReplaceTransformation().generate_spec("digits")

        mock_get_pattern.assert_called_once_with("digits")
        self.assertEqual(spec, {"pattern": r"\d+"})

    @patch("apps.jobs.transformations.extract.get_regex_pattern")
    def test_extract_uses_regex_path(self, mock_get_pattern):
        mock_get_pattern.return_value = r"\d+"

        spec = ExtractTransformation().generate_spec("digits")

        mock_get_pattern.assert_called_once_with("digits")
        self.assertEqual(spec, {"pattern": r"\d+"})

    @patch(
        "apps.jobs.transformations.standardize_format.get_format_operation"
    )
    def test_standardize_format_uses_format_path_not_regex_path(
        self, mock_get_operation
    ):
        # The one strategy that must NOT go through get_regex_pattern —
        # this test would fail if someone accidentally wired it to the
        # wrong llm_utils function.
        mock_get_operation.return_value = "UPPER"

        spec = StandardizeFormatTransformation().generate_spec("shout it")

        mock_get_operation.assert_called_once_with("shout it")
        self.assertEqual(spec, {"operation": "UPPER"})


class ApplyOnRealSparkTests(TestCase):
    """
    apply() methods run actual Spark SQL functions against a
    DataFrame. Mocking Spark here would mean never actually checking
    that regexp_extract's group index, or the format-function mapping,
    are correct — so this uses a real local SparkSession instead.
    """

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        from pyspark.sql import SparkSession

        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("transformation-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        super().tearDownClass()

    def _job(self, **overrides):
        """A lightweight stand-in for a Job model instance — apply()
        only reads a couple of attributes off it, so a real DB row
        isn't needed."""
        from types import SimpleNamespace

        defaults = {"replacement_value": "", "output_column_name": None}
        defaults.update(overrides)
        return SimpleNamespace(**defaults)

    def test_find_replace_replaces_matched_text(self):
        df = self.spark.createDataFrame(
            [("call 555-1234 now",), ("no numbers here",)], ["notes"]
        )
        job = self._job(replacement_value="[REDACTED]")

        result = FindReplaceTransformation().apply(
            df, "notes", {"pattern": r"\d{3}-\d{4}"}, job
        )
        rows = [r["notes"] for r in result.collect()]

        self.assertEqual(
            rows, ["call [REDACTED] now", "no numbers here"]
        )

    def test_find_replace_with_empty_replacement_deletes_match(self):
        df = self.spark.createDataFrame([("abc123def",)], ["value"])
        job = self._job(replacement_value="")

        result = FindReplaceTransformation().apply(
            df, "value", {"pattern": r"\d+"}, job
        )

        self.assertEqual(result.collect()[0]["value"], "abcdef")

    def test_extract_creates_new_column_with_default_name(self):
        df = self.spark.createDataFrame([("id: 4821",)], ["notes"])
        job = self._job(output_column_name=None)

        result = ExtractTransformation().apply(
            df, "notes", {"pattern": r"\d+"}, job
        )

        self.assertIn("notes_extracted", result.columns)
        self.assertEqual(result.collect()[0]["notes_extracted"], "4821")

    def test_extract_creates_new_column_with_custom_name(self):
        df = self.spark.createDataFrame([("id: 4821",)], ["notes"])
        job = self._job(output_column_name="extracted_id")

        result = ExtractTransformation().apply(
            df, "notes", {"pattern": r"\d+"}, job
        )

        self.assertIn("extracted_id", result.columns)
        self.assertNotIn("notes_extracted", result.columns)
        self.assertEqual(result.collect()[0]["extracted_id"], "4821")

    def test_extract_no_match_yields_empty_string_not_null(self):
        # regexp_extract's documented behavior on no match is "" — the
        # README calls this out explicitly (the AU/US area-code
        # example): "correct-but-empty extraction", not a crash and
        # not a silent None. Locking in that this is what actually
        # happens.
        df = self.spark.createDataFrame([("no digits at all",)], ["notes"])
        job = self._job()

        result = ExtractTransformation().apply(
            df, "notes", {"pattern": r"\d+"}, job
        )

        self.assertEqual(result.collect()[0]["notes_extracted"], "")

    def test_standardize_format_upper(self):
        df = self.spark.createDataFrame([("hello world",)], ["name"])
        result = StandardizeFormatTransformation().apply(
            df, "name", {"operation": "UPPER"}, self._job()
        )
        self.assertEqual(result.collect()[0]["name"], "HELLO WORLD")

    def test_standardize_format_lower(self):
        df = self.spark.createDataFrame([("HELLO WORLD",)], ["name"])
        result = StandardizeFormatTransformation().apply(
            df, "name", {"operation": "LOWER"}, self._job()
        )
        self.assertEqual(result.collect()[0]["name"], "hello world")

    def test_standardize_format_initcap(self):
        df = self.spark.createDataFrame([("hello world",)], ["name"])
        result = StandardizeFormatTransformation().apply(
            df, "name", {"operation": "INITCAP"}, self._job()
        )
        self.assertEqual(result.collect()[0]["name"], "Hello World")

    def test_standardize_format_trim(self):
        df = self.spark.createDataFrame([("  hello world  ",)], ["name"])
        result = StandardizeFormatTransformation().apply(
            df, "name", {"operation": "TRIM"}, self._job()
        )
        self.assertEqual(result.collect()[0]["name"], "hello world")

    def test_standardize_format_does_not_mutate_other_columns(self):
        # apply() should only touch target_column — a regression here
        # (e.g. someone changing withColumn's target by mistake) would
        # silently corrupt unrelated data.
        df = self.spark.createDataFrame(
            [("hello", "untouched")], ["name", "other"]
        )
        result = StandardizeFormatTransformation().apply(
            df, "name", {"operation": "UPPER"}, self._job()
        )
        row = result.collect()[0]
        self.assertEqual(row["name"], "HELLO")
        self.assertEqual(row["other"], "untouched")
