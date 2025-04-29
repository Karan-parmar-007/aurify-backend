from flask import Flask
import os
from pymongo import MongoClient  # Import MongoClient for MongoDB
from flask_cors import CORS
from app.utils.logger import logger


def create_app():
    app = Flask(__name__)
    
    # Load MongoDB configuration from environment variables
    app.config['MONGO_URI'] = os.getenv("MONGO_URI")
    app.config['MONGO_DBNAME'] = os.getenv("MONGO_DBNAME", "your_default_database")  # Set a default or adjust as needed

    # Initialize MongoDB client and attach to app
    if app.config['MONGO_URI']:
        app.mongo = MongoClient(app.config['MONGO_URI'])
    else:
        logger.error("MONGO_URI not set in environment variables.")
        raise ValueError("MONGO_URI must be set to connect to MongoDB.")
    
    app.secret_key = os.getenv("SECRET_KEY", "your_default_secret_key")  # Ensure SECRET_KEY is set



    # Enable CORS
    CORS(app, origins=["http://localhost:5173"], supports_credentials=True)

    # Register Blueprints
    from app.blueprints.user import user_bp
    from app.blueprints.auth import auth_bp
    from app.blueprints.admin import admin_bp
    from app.blueprints.project import project_bp
    from app.blueprints.dataset import dataset_bp


    app.register_blueprint(user_bp, url_prefix='/api/v1/user')
    app.register_blueprint(auth_bp, url_prefix='/api/v1/auth')
    app.register_blueprint(admin_bp, url_prefix='/api/v1/admin')
    app.register_blueprint(project_bp, url_prefix='/api/v1/project')
    app.register_blueprint(dataset_bp, url_prefix='/api/v1/dataset')

    # Log registered URLs for debugging
    logger.info("Registered URLs:")
    for rule in app.url_map.iter_rules():
        logger.info(rule)

    return app