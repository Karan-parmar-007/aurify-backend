from app.utils.db import db
from datetime import datetime
from pymongo.errors import PyMongoError
from bson import ObjectId
from app.utils.logger import logger
from app.utils.timestamps import add_timestamps

class ProjectModel:
    """MongoDB model class for handling project operations and data management"""
    
    def __init__(self):
        """Initialize the ProjectModel with the 'projects' collection"""
        self.collection = db["projects"]

    def create_project(self, user_id, name, file_path):
        """Create a new project in the database with initial parameters
        
        Args:
            user_id (str): ID of the user who owns the project
            name (str): Name of the project
            file_path (str): Path to the project file
            
        Returns:
            str|None: Inserted project ID as string, or None on error
        """
        try:
            project_data = {
                "user_id": ObjectId(user_id),
                "name": name,
                "file_path": file_path,
                "datatype_mapping": [],
                "name_of_the_column_with_tags": "",
                "name_of_the_column_with_tag_type": ""
            }
            project_data = add_timestamps(project_data)
            result = self.collection.insert_one(project_data)
            return str(result.inserted_id)
        except PyMongoError as e:
            logger.error(f"Database error while creating project: {e}")
            return None

    def update_project(self, project_id, datatype_mapping, column_with_tags, column_with_tag_type):
        """Update project with datatype mapping and column information
        
        Args:
            project_id (str): ID of the project to update
            datatype_mapping (list): List of dictionaries containing column_name and datatype
            column_with_tags (str): Name of the column containing tags
            column_with_tag_type (str): Name of the column containing tag types
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            update_data = {
                "datatype_mapping": datatype_mapping,
                "name_of_the_column_with_tags": column_with_tags,
                "name_of_the_column_with_tag_type": column_with_tag_type
            }
            update_data = add_timestamps(update_data, is_update=True)
            
            result = self.collection.update_one(
                {"_id": ObjectId(project_id)},
                {"$set": update_data}
            )
            return result.modified_count > 0
        except PyMongoError as e:
            logger.error(f"Database error while updating project: {e}")
            return False

    def delete_project(self, project_id):
        """Delete a project from the database
        
        Args:
            project_id (str): ID of the project to delete
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            result = self.collection.delete_one({"_id": ObjectId(project_id)})
            return result.deleted_count > 0
        except PyMongoError as e:
            logger.error(f"Database error while deleting project: {e}")
            return False 