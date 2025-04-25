from app.utils.db import db
from datetime import datetime
from pymongo.errors import PyMongoError
from bson import ObjectId
from app.utils.logger import logger
from app.utils.timestamps import add_timestamps

class VersionModel:
    """MongoDB model class for handling version operations and data management"""
    
    def __init__(self):
        """Initialize the VersionModel with the 'versions' collection"""
        self.collection = db["versions"]

    def create_version(self, user_id, description):
        """Create a new version in the database with initial parameters
        
        Args:
            user_id (str): ID of the user who owns the version
            description (str): Description of the version
            
        Returns:
            str|None: Inserted version ID as string, or None on error
        """
        try:
            version_data = {
                "user_id": ObjectId(user_id),
                "description": description,
                "files_created": []
            }
            version_data = add_timestamps(version_data)
            result = self.collection.insert_one(version_data)
            return str(result.inserted_id)
        except PyMongoError as e:
            logger.error(f"Database error while creating version: {e}")
            return None

    def update_version(self, version_id, files_created):
        """
        Update version with files created information
        
        Args:
            version_id (str): ID of the version to update
            files_created (list): List of dictionaries containing file_name and file_path
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            update_data = {
                "files_created": files_created
            }
            update_data = add_timestamps(update_data, is_update=True)
            
            result = self.collection.update_one(
                {"_id": ObjectId(version_id)},
                {"$set": update_data}
            )
            return result.modified_count > 0
        except PyMongoError as e:
            logger.error(f"Database error while updating version: {e}")
            return False

    def delete_version(self, version_id):
        """
        Delete a version from the database
        
        Args:
            version_id (str): ID of the version to delete
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            result = self.collection.delete_one({"_id": ObjectId(version_id)})
            return result.deleted_count > 0
        except PyMongoError as e:
            logger.error(f"Database error while deleting version: {e}")
            return False 