# Getting Started

## 1. Create python virtual environment

```bash
python3 -m venv .venv  #MacOS/Linux

python -m venv .venv #Windows
```

## 2. Activate virtual environment

```bash
source .venv/bin/activate #MacOS/Linux

.venv/Scripts/activate #Windows
```

## 3. Update pip
```bash
pip install --upgrade pip
```

## 4. Install dependencies
```bash
pip install -r requirements.txt
```
# Running Modules
Make sure to set up you .env file with ENVIRONMENT=DEVELOPMENT

## Local Server
```bash
python -m server
```

## Local Chron Execution
```bash
python -m chron
```

# Developer Notes
## Naming Conventions

- Varibable names: snake_case
- Class names: PascalCase

## Abbreviations

| Abbreviation | Meaning             |
| ------------ | ------------------- |
| bmk          | Benchmark           |
| xs           | Excess              |
| port         | Portfolio           |
| rf           | Risk Free           |
| vol          | Volatility          |
| ir           | Information Ratio   |
| cum          | Cummulative         |
| ci           | Confidence Interval |
| xf           | Transform           |
