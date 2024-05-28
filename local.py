from api.api import app
from flask_cors import CORS

if __name__ == "__main__":
    app.run(debug=True, port=8080)
