from api.api import app
from waitress import serve
from flask_cors import CORS

if __name__ == "__main__":
    serve(app, host="0.0.0.0", port=5000, url_scheme="https")
