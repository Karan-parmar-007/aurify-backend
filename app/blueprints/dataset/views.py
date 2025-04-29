from app.blueprints.dataset import dataset_bp
from app.models.project_model import ProjectModel
from app.models.version_model import VersionModel
from app.utils.logger import logger
from app.utils.timestamps import add_timestamps
from app.models.user_model import UserModel
import os
from werkzeug.utils import secure_filename
from flask import request, jsonify, send_file
import pandas as pd

# Initialize models
project_model = ProjectModel()
user_model = UserModel()

UPLOAD_FOLDER = os.path.join(os.getcwd(), 'datasets')
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

def save_file(file, filename, project_name):
    """Save uploaded file to a project-specific folder in the datasets directory
    
    Args:
        file: File object from request
        filename: Desired filename
        project_name: Name of the project (used as folder name)
        
    Returns:
        tuple: (bool, str) - (success status, file path or error message)
    """
    try:
        # Secure the project name and filename
        secure_project_name = secure_filename(project_name)
        secure_name = secure_filename(filename)
        
        # Create project-specific folder path
        project_folder = os.path.join(UPLOAD_FOLDER, secure_project_name)
        
        # Check if project folder already exists
        if os.path.exists(project_folder):
            return False, "A project with this name already exists. Please choose a different project name."
        
        # Create project folder
        os.makedirs(project_folder)
        
        # Create file path within project folder
        file_path = os.path.join(project_folder, secure_name)
        
        # Save the file
        file.save(file_path)
        return True, file_path
    except Exception as e:
        logger.error(f"Error saving file: {str(e)}")
        return False, "Error saving file"
    
@dataset_bp.route('/get_column_names', methods=['GET'])
def get_column_names():
    """Get column names from the uploaded dataset file
    
    Returns:
        JSON response with column names or error message
    """
    project_id = request.args.get('project_id')
    project = project_model.get_project(project_id)
    
    if not project:
        return jsonify({"error": "Project not found"}), 404
    
    file_path = project.get("file_path")
    
    if not file_path or not os.path.exists(file_path):
        return jsonify({"error": "File not found"}), 404
    
    try:
        # Read the dataset and extract column names
        if file_path.endswith(".xlsx"):
            df = pd.read_excel(file_path, dtype=str, nrows=1)  # Read only the first row
        elif file_path.endswith(".csv"):
            df = pd.read_csv(file_path, dtype=str, nrows=1)  # Read only the first row
        else:
            return jsonify({"error": "Unsupported file format"}), 400

        # Extract column names
        column_names = df.columns.tolist()

        return jsonify({"column_names": column_names}), 200
    except Exception as e:
        logger.error(f"Error reading file: {str(e)}")
        return jsonify({"error": "Error reading file", "details": str(e)}), 500