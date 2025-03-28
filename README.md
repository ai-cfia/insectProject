# Insect Project

A Python application for generating and sending reports about insect
observations and comments.

## Prerequisites

- Python 3.12+
- Docker (optional, for containerized execution)

## Local Setup

1. Clone the repository
2. Create a virtual environment:

   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

4. Copy `.env.template` to `.env` and fill in your configuration:

   ```bash
   cp .env.template .env
   ```

## Running Locally

Generate a comments report:

```bash
python run.py comments
```

Generate an observations report:

```bash
python run.py observations
```

## Running with Docker

Build the image:

```bash
docker build -t insect-project .
```

Run the container:

```bash
# For observations report (default)
docker run --env-file .env insect-project

# For comments report
docker run --env-file .env insect-project comments
```

## Environment Variables

Make sure to set up your environment variables in the `.env` file. See
`.env.template` for required variables.
