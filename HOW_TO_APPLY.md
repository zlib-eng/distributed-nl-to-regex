# Adding this test suite to your repo

1. In your actual repo, delete the placeholder file:
   backend/apps/jobs/tests.py

2. Copy this into your repo, preserving paths:
   backend/apps/jobs/tests/__init__.py
   backend/apps/jobs/tests/test_llm_utils.py
   backend/apps/jobs/tests/test_transformations.py
   backend/config/settings_test.py

3. Run it (from backend/, inside your existing dev container is easiest,
   since it already has all requirements.txt deps installed):

   docker compose exec web python manage.py test --settings=config.settings_test

   Or locally, if you have the deps installed:

   DJANGO_SECRET_KEY=x GEMINI_API_KEY=x python manage.py test --settings=config.settings_test

   No real Postgres, Redis, or Gemini API key is needed to run these —
   settings_test.py swaps in SQLite + local-memory cache, and the LLM
   client is mocked in every test. The Spark tests in
   test_transformations.py do spin up a real local Spark session
   (~10-20s), same as your app does — no mocking of Spark itself.

4. Verified passing in a clean environment: 43/43 tests, ~27s total
   (dominated by Spark session startup).

Not yet covered (next priority if you want to keep going):
  - apps/jobs/tasks.py — the job state machine (mock generate_spec/apply,
    assert status transitions + retry-vs-fail-immediately behavior)
  - apps/jobs/views.py — DRF APITestCase for the job creation/status/
    cancel endpoints
  - apps/jobs/spark_utils.py — read_file/write_output, ColumnNotFoundError
