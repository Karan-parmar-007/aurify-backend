import os
from flask import request, jsonify
from app.blueprints.project import project_bp
from app.models.project_model import ProjectModel
from app.models.user_model import UserModel
from app.utils.logger import logger
from werkzeug.utils import secure_filename

# Initialize models
project_model = ProjectModel()
user_model = UserModel()

# Configure upload folder
UPLOAD_FOLDER = os.path.join(os.getcwd(), 'dataset')
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

def save_file(file, filename):
    """Save uploaded file to the dataset folder
    
    Args:
        file: File object from request
        filename: Desired filename
        
    Returns:
        tuple: (bool, str) - (success status, file path or error message)
    """
    try:
        # Secure the filename
        secure_name = secure_filename(filename)
        file_path = os.path.join(UPLOAD_FOLDER, secure_name)
        
        # Check if file already exists
        if os.path.exists(file_path):
            return False, "File with this name already exists. Please rename the file."
        
        # Save the file
        file.save(file_path)
        return True, file_path
    except Exception as e:
        logger.error(f"Error saving file: {str(e)}")
        return False, "Error saving file"

@project_bp.route('/upload_dataset', methods=['POST'])
def upload_dataset():
    """Upload a file and create a new project
    
    Request form data:
    - file: The file to upload
    - name: Project name
    - user_id: User ID who owns the project
    
    Returns:
        JSON response with status and project details
    """
    try:
        # Check if file is present in request
        if 'file' not in request.files:
            return jsonify({
                'status': 'error',
                'message': 'No file part in the request'
            }), 400
            
        file = request.files['file']
        if file.filename == '':
            return jsonify({
                'status': 'error',
                'message': 'No file selected'
            }), 400
            
        # Get other form data
        name = request.form.get('name')
        user_id = request.form.get('user_id')
        
        # Validate required fields
        if not all([name, user_id]):
            return jsonify({
                'status': 'error',
                'message': 'Missing required fields: name and user_id'
            }), 400
            
        # Save file and get path
        success, result = save_file(file, file.filename)
        if not success:
            return jsonify({
                'status': 'error',
                'message': result
            }), 400
            
        # Create project
        project_id = project_model.create_project(
            user_id=user_id,
            name=name,
            file_path=result
        )
        
        if project_id:
            # Add project to user's projects array
            user_model.add_project(user_id, name, project_id)
            
            return jsonify({
                'status': 'success',
                'message': 'File uploaded and project created successfully',
                'project_id': project_id
            }), 201
        else:
            # If project creation fails, delete the uploaded file
            os.remove(result)
            return jsonify({
                'status': 'error',
                'message': 'Failed to create project'
            }), 500
            
    except Exception as e:
        logger.error(f"Error in upload_file: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred'
        }), 500

@project_bp.route('/update_project/<project_id>', methods=['PUT'])
def update_project(project_id):
    """Update project datatype mapping
    
    Args:
        project_id (str): ID of the project to update
        
    Request body:
    {
        "datatype_mapping": [
            {"column_name": "col1", "datatype": "string"},
            {"column_name": "col2", "datatype": "number"}
        ]
    }
    
    Returns:
        JSON response with status
    """
    try:
        data = request.get_json()
        
        # Validate required fields
        if 'datatype_mapping' not in data:
            return jsonify({
                'status': 'error',
                'message': 'Missing required field: datatype_mapping'
            }), 400
            
        # Update project
        success = project_model.update_project(project_id, data['datatype_mapping'])
        
        if success:
            return jsonify({
                'status': 'success',
                'message': 'Project updated successfully'
            }), 200
        else:
            return jsonify({
                'status': 'error',
                'message': 'Failed to update project'
            }), 500
            
    except Exception as e:
        logger.error(f"Error in update_project: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred'
        }), 500

@project_bp.route('delete_project/<project_id>', methods=['DELETE'])
def delete_project(project_id):
    """Delete a project and its associated file
    
    Args:
        project_id (str): ID of the project to delete
        
    Returns:
        JSON response with status
    """
    try:
        # Get project details before deletion
        project = project_model.get_project(project_id)
        if not project:
            return jsonify({
                'status': 'error',
                'message': 'Project not found'
            }), 404
            
        # Delete file from local storage
        try:
            if os.path.exists(project['file_path']):
                os.remove(project['file_path'])
        except Exception as e:
            logger.error(f"Error deleting file: {str(e)}")
            return jsonify({
                'status': 'error',
                'message': 'Error deleting file'
            }), 500
            
        # Delete project from database
        success = project_model.delete_project(project_id)
        if success:
            # Remove project from user's projects array
            user_model.remove_project(project['user_id'], project_id)
            
            return jsonify({
                'status': 'success',
                'message': 'Project deleted successfully'
            }), 200
        else:
            return jsonify({
                'status': 'error',
                'message': 'Failed to delete project'
            }), 500
            
    except Exception as e:
        logger.error(f"Error in delete_project: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred'
        }), 500 