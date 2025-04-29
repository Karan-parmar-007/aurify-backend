import os
import pandas as pd
from flask import request, jsonify, send_file
from app.blueprints.project import project_bp
from app.models.project_model import ProjectModel
from app.models.user_model import UserModel
from app.utils.logger import logger
from werkzeug.utils import secure_filename
from app.models.version_model import VersionModel

# Initialize models
project_model = ProjectModel()
user_model = UserModel()

# Configure upload folder
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

@project_bp.route('/upload_dataset', methods=['POST'])
def upload_dataset():
    """Upload a file, create a project, process the dataset, and manage versions."""
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
        remove_duplicates = request.form.get('remove_duplicates', 'false').lower() == 'true'

        # Validate required fields
        if not all([name, user_id]):
            return jsonify({
                'status': 'error',
                'message': 'Missing required fields: name and user_id'
            }), 400

        # Save file and get path
        success, result = save_file(file, file.filename, name)
        if not success:
            return jsonify({
                'status': 'error',
                'message': result
            }), 400

        # Step 1: Create the project in the database
        project_id = project_model.create_project(
            user_id=user_id,
            name=name,
            file_path=result,
            remove_duplicates=remove_duplicates
        )
        if not project_id:
            os.remove(result)
            return jsonify({
                'status': 'error',
                'message': 'Failed to create project'
            }), 500

        # Step 2: Create version 0
        version_model = VersionModel()
        version_0_id = version_model.create_version(
            project_id=project_id,
            description="Initial version",
            files_path=result
        )


        if not version_0_id:
            os.remove(result)
            project_model.delete_project(project_id)
            return jsonify({
                'status': 'error',
                'message': 'Failed to create version 0'
            }), 500

        version_update = project_model.append_version_info(
            project_id=project_id,
            version_entry={"v0": version_0_id}
        )

        # Step 3: Read the dataset
        try:
            if result.endswith('.xlsx'):
                df = pd.read_excel(result, dtype=str)
            elif result.endswith('.csv'):
                df = pd.read_csv(result, dtype=str)
            else:
                return jsonify({
                    'status': 'error',
                    'message': 'Unsupported file format'
                }), 400
        except Exception as e:
            logger.error(f"Error reading file: {str(e)}")
            return jsonify({
                'status': 'error',
                'message': 'Error reading the file',
                'details': str(e)
            }), 500

        # Step 4: Remove empty rows
        df.dropna(how='all', inplace=True)

        # Step 5: Remove duplicates if required
        if remove_duplicates:
            df.drop_duplicates(inplace=True)

        # Step 6: Save the processed dataset as a new version
        # Rename the file with _v1 at the end
        original_filename = os.path.basename(result)
        filename_without_ext, ext = os.path.splitext(original_filename)
        new_filename = f"{filename_without_ext}_v1{ext}"

        # Save the new file in the dataset/projectname/ folder
        project_folder = os.path.dirname(result)
        new_file_path = os.path.join(project_folder, new_filename)
        print(f"New file path: {new_file_path}")
        if ext == '.xlsx':
            df.to_excel(new_file_path, index=False, engine='openpyxl')
        elif ext == '.csv':
            df.to_csv(new_file_path, index=False, encoding='utf-8')

        # Step 7: Create version 1
        version_1_id = version_model.create_version(
            project_id=project_id,
            description="Processed version with cleaned data",
            files_path=new_file_path,
            version_number= 1
        )
        if not version_1_id:
            os.remove(new_file_path)
            return jsonify({
                'status': 'error',
                'message': 'Failed to create version 1'
            }), 500

        # Step 8: Update the project with the new file path
        project_updated = project_model.update_all_fields(
            project_id=project_id,
            update_fields={
                "file_path": new_file_path,
                "version_number": 1,
                
            }
        )

        if not project_updated:
            os.remove(new_file_path)
            return jsonify({
                'status': 'error',
                'message': 'Failed to update project with new file path'
            }), 500

        version_update_1 = project_model.append_version_info(
            project_id=project_id,
            version_entry={"v1": version_1_id}
        )

        # Add project to user's projects array
        user_model.add_project(user_id, name, project_id)

        return jsonify({
            'status': 'success',
            'message': 'File uploaded, processed, and project created successfully',
            'project_id': project_id,
            'version_0_id': version_0_id,
            'version_1_id': version_1_id
        }), 201

    except Exception as e:
        logger.error(f"Error in upload_dataset: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred',
            'details': str(e)
        }), 500

@project_bp.route('/update_project/<project_id>', methods=['PUT'])
def update_project(project_id):
    """
    Update project datatype mapping
    
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
            
        # Delete project folder and its contents
        try:
            project_folder = os.path.dirname(project['file_path'])
            if os.path.exists(project_folder):
                # Remove all files in the project folder
                for file in os.listdir(project_folder):
                    file_path = os.path.join(project_folder, file)
                    if os.path.isfile(file_path):
                        os.remove(file_path)
                # Remove the project folder itself
                os.rmdir(project_folder)
        except Exception as e:
            logger.error(f"Error deleting project folder: {str(e)}")
            return jsonify({
                'status': 'error',
                'message': 'Error deleting project folder'
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

@project_bp.route('/get_projects/<user_id>', methods=['GET'])
def get_projects(user_id):
    """Fetch all projects for a given user ID
    
    Args:
        user_id (str): ID of the user whose projects are to be fetched
        
    Returns:
        JSON response with project details or downloadable file
    """
    try:
        # Fetch projects from the database
        projects = project_model.get_projects_by_user(user_id)
        if not projects:
            return jsonify({
                'status': 'error',
                'message': 'No projects found for the user'
            }), 404
        
        # Return project details
        return jsonify({
            'status': 'success',
            'projects': projects
        }), 200
    except Exception as e:
        logger.error(f"Error in get_projects: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred'
        }), 500

@project_bp.route('/get_project_data/<project_id>', methods=['GET'])
def get_project_data(project_id):
    """
    Fetch project data, read the file, and return column names and first 10 rows
    
    Args:
        project_id (str): ID of the project to fetch data for
        
    Returns:
        JSON response with column names and first 10 rows of the file
    """
    try:
        # Fetch project details from the database
        project = project_model.get_project(project_id)
        if not project:
            return jsonify({
                'status': 'error',
                'message': 'Project not found'
            }), 404

        # Get the file path from the project data
        file_path = project.get('file_path')
        logger.debug(f"File path being used: {file_path}")
        
        if not file_path or not os.path.exists(file_path):
            logger.error(f"File does not exist at path: {file_path}")
            return jsonify({
                'status': 'error',
                'message': 'File not found'
            }), 404

        def clean_and_preview(file_path, num_rows=10, is_excel=False):
            """Helper function to clean and preview file content."""
            if is_excel:
                if file_path.endswith('.xlsx'):
                    engine = "openpyxl"
                elif file_path.endswith('.xls'):
                    engine = "xlrd"
                else:
                    raise ValueError("Unsupported Excel file extension")
                df = pd.read_excel(file_path, engine=engine, dtype=str, nrows=num_rows)
            else:
                try:
                    df = pd.read_csv(file_path, dtype=str, nrows=num_rows, encoding="utf-8")
                except UnicodeDecodeError:
                    df = pd.read_csv(file_path, dtype=str, nrows=num_rows, encoding="ISO-8859-1")

            # Convert column names to strings to handle datetime objects
            df.columns = [str(col) for col in df.columns]

            # Replace NaN with None for JSON compatibility
            df = df.where(pd.notnull(df), '')

            # Convert all values to strings to ensure JSON serialization
            df = df.astype(str).replace("nan", '')

            # Return preview as list of dictionaries
            return df.head(num_rows).to_dict(orient="records")

        # Read and preview the file
        try:
            if file_path.endswith(".xlsx"):
                try:
                    rows = clean_and_preview(file_path, num_rows=10, is_excel=True)
                except Exception as e:
                    logger.warning(f"Excel read failed, trying CSV fallback: {e}")
                    rows = clean_and_preview(file_path, num_rows=10, is_excel=False)
            elif file_path.endswith(".csv"):
                rows = clean_and_preview(file_path, num_rows=10, is_excel=False)
            else:
                return jsonify({
                    'status': 'error',
                    'message': 'Unsupported file format'
                }), 400
        except Exception as e:
            logger.error(f"Error reading file: {str(e)}")
            return jsonify({
                'status': 'error',
                'message': 'Error reading the file',
                'details': str(e)
            }), 500

        # Return the data to the frontend
        return jsonify({
            'status': 'success',
            'columns': list(rows[0].keys()) if rows else [],
            'rows': rows
        }), 200

    except Exception as e:
        logger.error(f"Error in get_project_data: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred',
            'details': str(e)
        }), 500

@project_bp.route('/download_file/<path:file_path>', methods=['GET'])
def download_file(file_path):
    """Download a file from the server
    
    Args:
        file_path (str): Path to the file to be downloaded
        
    Returns:
        File response or error message
    """
    try:
        # Normalize the file path
        normalized_path = os.path.normpath(file_path)

        # Ensure the file exists
        if not os.path.exists(normalized_path):
            return jsonify({
                'status': 'error',
                'message': 'File not found'
            }), 404
        
        # Send the file for download
        return send_file(normalized_path, as_attachment=True)
    except Exception as e:
        logger.error(f"Error in download_file: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred'
        }), 500
    
