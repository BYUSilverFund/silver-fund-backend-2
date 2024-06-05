### Python Virtual Envrionment

- create virtual envrionment

```
python -m venv .venv
```

- Activate virtual environment

```
source .venv/bin/activate //Mac

.venv/Scripts/activate //Windows
```

- Install dependencies

```
pip install -r requirements.txt
```

- How to deactivate virtual environment

```
deactivate
```

### Update Virtual Environment Dependencies

- In the event you pip install a new package onto the virtual environment

```
pip freeze > requirements.txt
```

- This adds the new package dependency to the requirements

### Run dev server

```
python main.py
```

### Execute test file

```
pytest functions/tests.py
```

### Special cases to think about when calculating performance

- Buying a stock, selling a month later, buying the same stock a month after that
- Selling half of a position
- Stock splits
- Doubling the size of a position
- Short positions
- Account withdrawals and deposits
- Cash returns
