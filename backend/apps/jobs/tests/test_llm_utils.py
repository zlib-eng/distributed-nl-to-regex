"""
Tests for apps/jobs/llm_utils.py

Two things live in this file that matter more than anywhere else in
the codebase:

1. Regex safety validation (`validate_regex`) — the only thing standing
   between an LLM hallucination and a catastrophic-backtracking regex
   running against a user's data.
2. The cache layer (`get_regex_pattern` / `get_format_operation`) — the
   README's central performance claim ("LLM calls are O(1) per job,
   never O(n) per row... zero LLM calls on a cache hit") lives here.
   These tests prove that claim instead of just asserting it.

The real Gemini client is never called in these tests — `client.models
.generate_content` is mocked throughout, so this suite runs in
milliseconds and needs no API key, network access, or quota.
"""
from unittest.mock import patch, MagicMock

from django.core.cache import cache
from django.test import TestCase, override_settings

from apps.jobs.llm_utils import (
    validate_regex,
    validate_format_operation,
    get_regex_pattern,
    get_format_operation,
    RegexGenerationError,
    FormatOperationError,
    TransformationSpecError,
)


LOCMEM_CACHE = {
    "default": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache"}
}


class ValidateRegexTests(TestCase):
    """Pure function, no mocking needed — feed it strings, check the outcome."""

    def test_accepts_simple_valid_pattern(self):
        self.assertEqual(validate_regex(r"\d{3}-\d{4}"), r"\d{3}-\d{4}")

    def test_strips_surrounding_whitespace(self):
        self.assertEqual(validate_regex("  \\d+  "), "\\d+")

    def test_rejects_empty_string(self):
        with self.assertRaises(RegexGenerationError):
            validate_regex("")

    def test_rejects_whitespace_only_string(self):
        with self.assertRaises(RegexGenerationError):
            validate_regex("   ")

    def test_rejects_syntactically_invalid_regex(self):
        # Unbalanced parenthesis — re.compile itself will raise re.error
        with self.assertRaises(RegexGenerationError):
            validate_regex("(abc")

    def test_rejects_nested_quantifier_catastrophic_backtracking(self):
        # The exact shape called out in the README's limitations section
        with self.assertRaises(RegexGenerationError):
            validate_regex("(a+)+")

    def test_rejects_nested_quantifier_with_star(self):
        with self.assertRaises(RegexGenerationError):
            validate_regex("(a*)*")

    def test_allows_single_non_nested_quantifier(self):
        # A plain quantifier should NOT trip the nested-quantifier check
        self.assertEqual(validate_regex("a+b*c?"), "a+b*c?")

    def test_allows_grouped_pattern_without_nested_quantifier(self):
        # A group is fine as long as it isn't itself repeated
        self.assertEqual(validate_regex("(abc)"), "(abc)")

    def test_error_is_a_transformation_spec_error(self):
        # tasks.py only catches TransformationSpecError, not the
        # concrete subclass — this is the contract that lets tasks.py
        # stay ignorant of which transformation type failed.
        with self.assertRaises(TransformationSpecError):
            validate_regex("(a+)+")


class ValidateFormatOperationTests(TestCase):

    def test_accepts_each_allowed_operation(self):
        for op in ("UPPER", "LOWER", "INITCAP", "TRIM"):
            with self.subTest(op=op):
                self.assertEqual(validate_format_operation(op), op)

    def test_normalizes_case_and_whitespace(self):
        self.assertEqual(validate_format_operation("  upper  "), "UPPER")

    def test_rejects_empty_string(self):
        with self.assertRaises(FormatOperationError):
            validate_format_operation("")

    def test_rejects_none_value(self):
        with self.assertRaises(FormatOperationError):
            validate_format_operation(None)

    def test_none_token_is_a_deliberate_rejection_not_a_crash(self):
        # This is the "explicit rejection token" behavior from the
        # README's Failure Handling demo — the LLM saying NONE must
        # fail the job cleanly, not be treated as a valid operation.
        with self.assertRaises(FormatOperationError):
            validate_format_operation("NONE")

    def test_rejects_operation_outside_allow_list(self):
        # LLM hallucinating something plausible-sounding but unsupported
        with self.assertRaises(FormatOperationError):
            validate_format_operation("REVERSE")

    def test_error_is_a_transformation_spec_error(self):
        with self.assertRaises(TransformationSpecError):
            validate_format_operation("NONE")


@override_settings(CACHES=LOCMEM_CACHE)
class GetRegexPatternCacheTests(TestCase):
    """
    Proves the README's central performance claim: identical prompts
    hit the LLM at most once. Everything after that is a cache read.
    """

    def setUp(self):
        cache.clear()

    @patch("apps.jobs.llm_utils._call_llm_for_regex")
    def test_cache_miss_calls_llm_exactly_once(self, mock_call):
        mock_call.return_value = r"\d{3}-\d{4}"

        result = get_regex_pattern("phone numbers")

        self.assertEqual(result, r"\d{3}-\d{4}")
        mock_call.assert_called_once()

    @patch("apps.jobs.llm_utils._call_llm_for_regex")
    def test_identical_prompt_never_calls_llm_twice(self, mock_call):
        mock_call.return_value = r"\d{3}-\d{4}"

        first = get_regex_pattern("phone numbers")
        second = get_regex_pattern("phone numbers")
        third = get_regex_pattern("phone numbers")

        self.assertEqual([first, second, third], [r"\d{3}-\d{4}"] * 3)
        mock_call.assert_called_once()  # not 3 times

    @patch("apps.jobs.llm_utils._call_llm_for_regex")
    def test_cache_key_is_case_and_whitespace_insensitive(self, mock_call):
        # "phone numbers" and "  Phone Numbers  " should be the same
        # cache entry — the normalization happens in _cache_key.
        mock_call.return_value = r"\d{3}-\d{4}"

        get_regex_pattern("phone numbers")
        get_regex_pattern("  Phone Numbers  ")

        mock_call.assert_called_once()

    @patch("apps.jobs.llm_utils._call_llm_for_regex")
    def test_different_prompts_are_independent_cache_entries(self, mock_call):
        mock_call.side_effect = [r"\d{3}-\d{4}", r"[A-Z]{2}\d{4}"]

        phone = get_regex_pattern("phone numbers")
        postcode = get_regex_pattern("postcodes")

        self.assertEqual(phone, r"\d{3}-\d{4}")
        self.assertEqual(postcode, r"[A-Z]{2}\d{4}")
        self.assertEqual(mock_call.call_count, 2)

    @patch("apps.jobs.llm_utils._call_llm_for_regex")
    def test_invalid_llm_output_is_never_cached(self, mock_call):
        # A pattern that fails validate_regex must not poison the
        # cache — otherwise every future call for this prompt would
        # instantly re-raise without ever giving the LLM a chance to
        # produce something valid.
        mock_call.return_value = "(a+)+"  # catastrophic backtracking

        with self.assertRaises(RegexGenerationError):
            get_regex_pattern("do something dangerous")

        self.assertIsNone(cache.get(
            "llm_cache:" + __import__("hashlib").sha256(
                b"regex:do something dangerous"
            ).hexdigest()
        ))
        mock_call.assert_called_once()

    @patch("apps.jobs.llm_utils._call_llm_for_regex")
    def test_regex_and_format_prompts_use_separate_cache_namespaces(
        self, mock_regex_call
    ):
        # Same literal prompt text, different transformation type,
        # must not collide — this is exactly what the _cache_key
        # namespace parameter exists to prevent.
        mock_regex_call.return_value = r"\d+"

        with patch(
            "apps.jobs.llm_utils._call_llm_for_format"
        ) as mock_format_call:
            mock_format_call.return_value = "UPPER"

            regex_result = get_regex_pattern("numbers")
            format_result = get_format_operation("numbers")

            self.assertEqual(regex_result, r"\d+")
            self.assertEqual(format_result, "UPPER")
            mock_regex_call.assert_called_once()
            mock_format_call.assert_called_once()


@override_settings(CACHES=LOCMEM_CACHE)
class GetFormatOperationCacheTests(TestCase):

    def setUp(self):
        cache.clear()

    @patch("apps.jobs.llm_utils._call_llm_for_format")
    def test_cache_miss_calls_llm_exactly_once(self, mock_call):
        mock_call.return_value = "INITCAP"

        result = get_format_operation("title case the name")

        self.assertEqual(result, "INITCAP")
        mock_call.assert_called_once()

    @patch("apps.jobs.llm_utils._call_llm_for_format")
    def test_identical_prompt_never_calls_llm_twice(self, mock_call):
        mock_call.return_value = "INITCAP"

        get_format_operation("title case the name")
        get_format_operation("title case the name")

        mock_call.assert_called_once()

    @patch("apps.jobs.llm_utils._call_llm_for_format")
    def test_none_response_is_never_cached(self, mock_call):
        # A rejected ("NONE") request shouldn't be cached either —
        # same reasoning as the regex case above.
        mock_call.return_value = "NONE"

        with self.assertRaises(FormatOperationError):
            get_format_operation("make it sparkle")

        mock_call.assert_called_once()
        with self.assertRaises(FormatOperationError):
            # If this were wrongly cached, the second call would still
            # raise (since "NONE" always raises) — so instead we check
            # the LLM gets called again, proving nothing was cached.
            get_format_operation("make it sparkle")
        self.assertEqual(mock_call.call_count, 2)
