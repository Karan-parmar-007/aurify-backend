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

    def get_project(self, project_id):
        """Get a project by its ID
        
        Args:
            project_id (str): ID of the project to retrieve
            
        Returns:
            dict|None: Project data as dictionary, or None if not found or error
        """
        try:
            project = self.collection.find_one({"_id": ObjectId(project_id)})
            if project:
                project["_id"] = str(project["_id"])
                project["user_id"] = str(project["user_id"])
            return project
        except PyMongoError as e:
            logger.error(f"Database error while getting project: {e}")
            return None

    def create_project(self, user_id, name, file_path, remove_duplicates):
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
                "datatype_mapping": {},
                "remove_duplicates": remove_duplicates,
            }
            project_data = add_timestamps(project_data)
            result = self.collection.insert_one(project_data)
            return str(result.inserted_id)
        except PyMongoError as e:
            logger.error(f"Database error while creating project: {e}")
            return None

    def update_project(self, project_id, datatype_mapping):
        """Update project with datatype mapping information
        
        Args:
            project_id (str): ID of the project to update
            datatype_mapping (list): List of dictionaries containing column_name and datatype
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            update_data = {
                "datatype_mapping": datatype_mapping
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

    def get_projects_by_user(self, user_id):
        """Fetch all projects for a given user ID
        
        Args:
            user_id (str): ID of the user whose projects are to be fetched
            
        Returns:
            list: List of projects as dictionaries, or an empty list on error
        """
        try:
            projects = self.collection.find({"user_id": ObjectId(user_id)})
            project_list = []
            for project in projects:
                project["_id"] = str(project["_id"])
                project["user_id"] = str(project["user_id"])
                project_list.append(project)
            return project_list
        except PyMongoError as e:
            logger.error(f"Database error while fetching projects for user {user_id}: {e}")
            return []