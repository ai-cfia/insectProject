# Testing Guide

## Running Tests

```shell
# Run all tests with pytest
pytest tests/ -v

# Run all tests with unittest
python -m unittest discover tests -v

# Run specific test file with pytest
pytest tests/test_comments_report.py -v

# Run specific test file with unittest
python -m unittest tests/test_comments_report.py -v
```

## Test Structure

```text
tests/
├── test_comments_report.py    # Comments report generation
├── test_emails.py            # Email functionality
├── test_observations.py      # Observation data handling
├── test_observations_report.py # Observations report generation
├── test_preprocess.py        # Data preprocessing
├── test_settings.py          # Configuration settings
└── test_species.py           # Species-related functionality
```

## Writing Tests

```python
import unittest
from src.your_module import your_function

class TestYourFunction(unittest.TestCase):
    def setUp(self):
        self.test_data = {...}

    def test_specific_case(self):
        result = your_function(self.test_data)
        self.assertEqual(result, expected_value)
```

### Async Tests

```python
class TestAsyncFunction(unittest.IsolatedAsyncioTestCase):
    async def test_async_function(self):
        result = await your_async_function()
        self.assertEqual(result, expected_value)
```

## CI/CD

Tests run automatically on pull requests via GitHub Actions:

- pytest for testing
- ruff for linting
- markdown validation
- repository standards check

## Debugging

```shell
# Debug failing test with pytest
pytest tests/your_test.py -v --pdb

# Debug failing test with unittest
python -m unittest tests/your_test.py -v

# Generate coverage report
pytest tests/ --cov=src --cov-report=html
```

## Common Issues

1. Import errors: Ensure project root is in Python path
2. Async test failures: Use `IsolatedAsyncioTestCase`
3. Mock issues: Check mock setup and cleanup
