from api.api import app
from waitress import serve

if __name__ == "__main__":
    serve(app, host="127.0.0.1", port=8080, url_scheme="https")
