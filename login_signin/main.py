from flask import Flask
from authenticate.routes import auth_bp
from flask_jwt_extended import JWTManager
from config.settings import Config
from flask_swagger_ui import get_swaggerui_blueprint
from fetch_secret import access_secret_version
import uvicorn

app = Flask(__name__)

jwt_secret_key = access_secret_version(Config.PROJECT_ID, "jwt-secret")

# Set up app config
app.config["JWT_SECRET_KEY"] = jwt_secret_key
# Initialize JWT Manager
jwt = JWTManager(app)

# Register the authentication blueprint
app.register_blueprint(auth_bp, url_prefix="/auth")
# Swagger setup
SWAGGER_URL = "/swagger"  # Swagger UI route
API_URL = "/static/swagger.yaml"  # Path to the Swagger YAML file

# Create Swagger UI blueprint and register it
swaggerui_blueprint = get_swaggerui_blueprint(
    SWAGGER_URL,
    API_URL,
    config={"app_name": "Authentication System"},  # Swagger UI config
)
app.register_blueprint(swaggerui_blueprint, url_prefix=SWAGGER_URL)


@app.route("/")
def home():
    return "Welcome to the Authentication System"


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
