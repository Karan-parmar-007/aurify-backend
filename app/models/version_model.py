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

    def create_version(self, project_id, description, files_path="", version_number=0):
        """
        Create a new version in the database with initial parameters.

        Args:
            project_id (str): ID of the project associated with the version
            description (str): Description of the version
            files_path (str, optional): Path where files are stored. Defaults to empty string.

        Returns:
            str|None: Inserted version ID as string, or None on error
        """
        try:
            version_data = {
                "project_id": ObjectId(project_id),
                "description": description,
                "files_path": files_path,
                "version_number": version_number,
            }
            version_data = add_timestamps(version_data)
            result = self.collection.insert_one(version_data)
            return str(result.inserted_id)
        except PyMongoError as e:
            logger.error(f"Database error while creating version: {e}")
            return None

    def update_version(self, version_id, files_path):
        """
        Update version's files_path information.

        Args:
            version_id (str): ID of the version to update
            files_path (str): Updated file path

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            update_data = {
                "files_path": files_path
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
        Delete a version from the database.

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
