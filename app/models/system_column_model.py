from app.utils.db import db
from datetime import datetime
from pymongo.errors import PyMongoError
from bson import ObjectId
from app.utils.logger import logger
from app.utils.timestamps import add_timestamps

class SystemColumnModel:
    """MongoDB model class for handling system column operations and data management"""
    
    def __init__(self):
        """Initialize the SystemColumnModel with the 'system_columns' collection"""
        self.collection = db["system_columns"]

    def create_column(self, column_name, description, alt_names, asset_class):
        """Create a new system column in the database
        
        Args:
            column_name (str): Name of the column
            description (str): Description of the column
            alt_names (list): List of alternative names for the column
            asset_class (str): Asset class of the column
            
        Returns:
            str|None: Inserted column ID as string, or None on error
        """
        try:
            column_data = {
                "column_name": column_name,
                "description": description,
                "alt_names": alt_names,
                "asset_class": asset_class
            }
            column_data = add_timestamps(column_data)
            result = self.collection.insert_one(column_data)
            return str(result.inserted_id)
        except PyMongoError as e:
            logger.error(f"Database error while creating system column: {e}")
            return None

    def update_column(self, column_id, update_data):
        """Update a system column
        
        Args:
            column_id (str): ID of the column to update
            update_data (dict): Dictionary containing fields to update
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            update_data = add_timestamps(update_data, is_update=True)
            result = self.collection.update_one(
                {"_id": ObjectId(column_id)},
                {"$set": update_data}
            )
            return result.modified_count > 0
        except PyMongoError as e:
            logger.error(f"Database error while updating system column: {e}")
            return False

    def delete_column(self, column_id):
        """Delete a system column from the database
        
        Args:
            column_id (str): ID of the column to delete
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            result = self.collection.delete_one({"_id": ObjectId(column_id)})
            return result.deleted_count > 0
        except PyMongoError as e:
            logger.error(f"Database error while deleting system column: {e}")
            return False

    def get_all_columns(self):
        """Get all system columns from the database
        
        Returns:
            list|None: List of all columns, or None on error
        """
        try:
            columns = list(self.collection.find())
            # Convert ObjectId to string for JSON serialization
            for column in columns:
                column["_id"] = str(column["_id"])
            return columns
        except PyMongoError as e:
            logger.error(f"Database error while getting all system columns: {e}")
            return None

    def get_column(self, column_id):
        """Get a single system column by ID
        
        Args:
            column_id (str): ID of the column to retrieve
            
        Returns:
            dict|None: Column data as dictionary, or None if not found or error
        """
        try:
            column = self.collection.find_one({"_id": ObjectId(column_id)})
            if column:
                column["_id"] = str(column["_id"])
            return column
        except PyMongoError as e:
            logger.error(f"Database error while getting system column: {e}")
            return None 