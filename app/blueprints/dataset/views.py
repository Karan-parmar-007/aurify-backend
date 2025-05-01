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

@dataset_bp.route('/update_column_names', methods=['POST'])
def update_column_names():
    """
    Update column names in the dataset file based on the provided mapping.
    
    Form Data:
        - project_id: The ID of the project.
        - mapped_columns: A nested dictionary containing old column names as keys and new column names as values.
    
    Returns:
        JSON response with success message or error details.
    """
    try:
        # Parse form data
        project_id = request.form.get("project_id")
        mapped_columns = request.form.get("mapped_columns")

        if not project_id or not mapped_columns:
            return jsonify({"error": "Missing required fields: project_id or mapped_columns"}), 400

        # Convert mapped_columns from string to dictionary
        try:
            import json
            column_mapping = json.loads(mapped_columns)
        except json.JSONDecodeError:
            return jsonify({"error": "Invalid format for mapped_columns"}), 400

        # Filter out mappings where the new column name is an empty string
        filtered_mapping = {old: new for old, new in column_mapping.items() if new.strip()}

        # Step 1: Fetch the project details
        project = project_model.get_project(project_id)
        if not project:
            return jsonify({"error": "Project not found"}), 404

        file_path = project.get("file_path")
        if not file_path or not os.path.exists(file_path):
            return jsonify({"error": "File not found"}), 404

        # Step 2: Load the dataset
        try:
            if file_path.endswith(".xlsx"):
                df = pd.read_excel(file_path, dtype=str)
            elif file_path.endswith(".csv"):
                df = pd.read_csv(file_path, dtype=str)
            else:
                return jsonify({"error": "Unsupported file format"}), 400
        except Exception as e:
            logger.error(f"Error reading file: {str(e)}")
            return jsonify({"error": "Error reading file", "details": str(e)}), 500

        # Step 3: Update column names
        df.rename(columns=filtered_mapping, inplace=True)

        # Step 4: Save the updated dataset
        original_filename = os.path.basename(file_path)
        filename_without_ext, ext = os.path.splitext(original_filename)
        new_filename = f"{filename_without_ext.replace('_v1', '')}_v2{ext}"

        project_folder = os.path.dirname(file_path)
        new_file_path = os.path.join(project_folder, new_filename)

        if ext == ".xlsx":
            df.to_excel(new_file_path, index=False, engine="openpyxl")
        elif ext == ".csv":
            df.to_csv(new_file_path, index=False, encoding="utf-8")

        # Step 5: Add a new entry in the version model
        version_model = VersionModel()
        version_2_id = version_model.create_version(
            project_id=project_id,
            description="Updated column names",
            files_path=new_file_path,
            version_number=2
        )
        if not version_2_id:
            os.remove(new_file_path)
            return jsonify({"error": "Failed to create version 2"}), 500

        # Step 6: Update the project with the new file path and version
        update_success = project_model.update_all_fields(
            project_id=project_id,
            update_fields={
                "file_path": new_file_path,
                "version_number": 2
            }
        )
        if not update_success:
            os.remove(new_file_path)
            return jsonify({"error": "Failed to update project with new file path and version"}), 500

        return jsonify({
            "status": "success",
            "message": "Column names updated successfully",
            "new_file_path": new_file_path,
            "version_2_id": version_2_id
        }), 200

    except Exception as e:
        logger.error(f"Error in update_column_names: {str(e)}")
        return jsonify({"error": "An unexpected error occurred", "details": str(e)}), 500
    
@dataset_bp.route('/partition_by_tags', methods=["POST"])
def partition_by_tags():
    '''
    Partition the latest file from project and create separate versions for each tag, including untagged.
    '''
    try:
        data = request.json
        project_id = data.get('project_id')
        if not project_id:
            return jsonify({"error": "Missing Project ID"}), 400

        # Step 1: Fetch the project details
        project = project_model.get_project(project_id)
        if not project:
            return jsonify({"error": "Project not found"}), 404

        file_path = project.get("file_path")
        if not file_path or not os.path.exists(file_path):
            return jsonify({"error": "File not found"}), 404

        tag_column = 'Tags'
        tag_type_column = 'Tag Type'
        project_name = project.get("name", f"project_{project_id}")

        # Step 2: Load the Dataset
        try:
            if file_path.endswith(".xlsx"):
                df = pd.read_excel(file_path, dtype=str)
                ext = ".xlsx"
            elif file_path.endswith(".csv"):
                df = pd.read_csv(file_path, dtype=str)
                ext = ".csv"
            else:
                return jsonify({"error": "Unsupported file format"}), 400
        except Exception as e:
            logger.error(f"Error reading file: {str(e)}")
            return jsonify({"error": "Error reading file", "details": str(e)}), 500

        # Step 3: Check if Tags and Tag Type columns exist
        if tag_column not in df.columns:
            return jsonify({"error": f"Tags column not found in file {df.columns}"}), 400
        if tag_type_column not in df.columns:
            return jsonify({"error": f"Tag Type column not found in file"}), 400

        # Step 4: Partition and Save Files
        version_model = VersionModel()
        sub_versions = []
        tag_groups = {}
        project_folder = os.path.join(UPLOAD_FOLDER, secure_filename(project_name))
        if not os.path.exists(project_folder):
            os.makedirs(project_folder)

        grouped = df.groupby([tag_column, tag_type_column], dropna=False)
        version_counter = 1

        for (tag, tag_type), group in grouped:
            tag_name = str(tag) if pd.notna(tag) and tag != "" else "Untagged"
            tag_type_name = str(tag_type) if pd.notna(tag_type) and tag_type != "" else "Unknown"
            file_base = f"{tag_name}_{tag_type_name}_v2.{version_counter}{ext}"
            file_save_path = os.path.join(project_folder, file_base)

            # Save the partitioned file
            if ext == ".xlsx":
                group.to_excel(file_save_path, index=False, engine="openpyxl")
            else:
                group.to_csv(file_save_path, index=False, encoding="utf-8")

            # Create a new version for this partition
            version_number = float(f"2.{version_counter}")
            version_id = version_model.create_version(
                project_id=project_id,
                description=f"Partitioned by {tag_name} - {tag_type_name}",
                files_path=file_save_path,
                version_number=version_number
            )
            if version_id:
                sub_versions.append({
                    "version": version_id,
                    "tag": tag_name,
                    "tag_type": tag_type_name,
                    "file_path": file_save_path,
                    "version_number": version_number
                })

            # Update tag_groups info
            tag_groups[f"{tag_name}_{tag_type_name}"] = {
                "entries": int(len(group)),
                "tag_type": tag_type_name
            }
            version_counter += 1

        # Step 5: Update the project with sub_versions and file_path
        project_model.update_all_fields(
            project_id=project_id,
            update_fields={
                "file_path": "sub_versions",
                "sub_versions": sub_versions,
                "version_number": 2
            }
        )

        return jsonify({
            "status": "success",
            "tag_groups": tag_groups
        }), 200

    except Exception as e:
        logger.error(f"Error in partition_by_tags: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"Server error: {str(e)}"}), 500




