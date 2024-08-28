from api.api import app
from waitress import serve

if __name__ == "__main__":
    # this runs the production server
    serve(app, host="0.0.0.0", port=5000, url_scheme="https")
