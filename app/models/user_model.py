from app.utils.db import db
from pymongo.errors import PyMongoError
from bson import ObjectId
from app.utils.logger import logger
from app.utils.timestamps import add_timestamps  # ⬅️ Importing utility

class UserModel:
    """MongoDB model class for handling user operations and data management"""
    
    def __init__(self):
        self.collection = db["users"]

    def create_user(self, name, email, password):
        try:
            user_data = {
                "name": name,
                "email": email,
                "password": password,
                "projects": [],
            }
            user_data = add_timestamps(user_data)
            result = self.collection.insert_one(user_data)
            return str(result.inserted_id)
        except PyMongoError as e:
            logger.error(f"Database error while creating user: {e}")
            return None

    def verify_user(self, email, password):
        """Verify user credentials
        
        Args:
            email (str): User's email
            password (str): User's password
            
        Returns:
            tuple: (bool, str) - (True/False, user_id if successful)
        """
        try:
            user = self.collection.find_one({"email": email})
            if user and user.get("password") == password:
                return True, str(user["_id"])
            return False, None
        except PyMongoError as e:
            logger.error(f"Database error while verifying user: {e}")
            return False, None

    def check_user_exists(self, email):
        try:
            user = self.collection.find_one({"email": email})
            return user is not None
        except PyMongoError as e:
            logger.error(f"Database error while checking user existence: {e}")
            return False

    def add_project(self, user_id, project_name, project_id):
        try:
            result = self.collection.update_one(
                {"_id": ObjectId(user_id)},
                {
                    "$push": {"projects": {"name": project_name, "project_id": project_id}},
                    "$set": add_timestamps({}, is_update=True)
                }
            )
            return result.modified_count > 0
        except PyMongoError as e:
            logger.error(f"Database error while adding project: {e}")
            return False

    def remove_project(self, user_id, project_id):
        try:
            result = self.collection.update_one(
                {"_id": ObjectId(user_id)},
                {
                    "$pull": {"projects": {"project_id": project_id}},
                    "$set": add_timestamps({}, is_update=True)
                }
            )
            return result.modified_count > 0
        except PyMongoError as e:
            logger.error(f"Database error while removing project: {e}")
            return False

    def update_project(self, user_id, project_id, new_project_name):
        try:
            result = self.collection.update_one(
                {"_id": ObjectId(user_id), "projects.project_id": project_id},
                {
                    "$set": {
                        "projects.$.name": new_project_name,
                        **add_timestamps({}, is_update=True)
                    }
                }
            )
            return result.modified_count > 0
        except PyMongoError as e:
            logger.error(f"Database error while updating project: {e}")
            return False

    def add_version_info(self, user_id, version_number, version_id):
        try:
            result = self.collection.update_one(
                {"_id": ObjectId(user_id)},
                {
                    "$push": {"version_info": {"version_number": version_number, "version_id": version_id}},
                    "$set": add_timestamps({}, is_update=True)
                }
            )
            return result.modified_count > 0
        except PyMongoError as e:
            logger.error(f"Database error while adding version info: {e}")
            return False

    def remove_version_info(self, user_id, version_id):
        try:
            result = self.collection.update_one(
                {"_id": ObjectId(user_id)},
                {
                    "$pull": {"version_info": {"version_id": version_id}},
                    "$set": add_timestamps({}, is_update=True)
                }
            )
            return result.modified_count > 0
        except PyMongoError as e:
            logger.error(f"Database error while removing version info: {e}")
            return False

    def update_version_info(self, user_id, version_id, new_version_number):
        try:
            result = self.collection.update_one(
                {"_id": ObjectId(user_id), "version_info.version_id": version_id},
                {
                    "$set": {
                        "version_info.$.version_number": new_version_number,
                        **add_timestamps({}, is_update=True)
                    }
                }
            )
            return result.modified_count > 0
        except PyMongoError as e:
            logger.error(f"Database error while updating version info: {e}")
            return False

    def update_version_number(self, user_id, new_version_number):
        try:
            result = self.collection.update_one(
                {"_id": ObjectId(user_id)},
                {
                    "$set": {
                        "version_number": new_version_number,
                        **add_timestamps({}, is_update=True)
                    }
                }
            )
            return result.modified_count > 0
        except PyMongoError as e:
            logger.error(f"Database error while updating version number: {e}")
            return False

    def delete_user(self, user_id):
        try:
            result = self.collection.delete_one({"_id": ObjectId(user_id)})
            return result.deleted_count > 0
        except PyMongoError as e:
            logger.error(f"Database error while deleting user: {e}")
            return False

    def update_user(self, user_id, update_data):
        try:
            update_data = add_timestamps(update_data, is_update=True)
            result = self.collection.update_one(
                {"_id": ObjectId(user_id)},
                {"$set": update_data}
            )
            return result.modified_count > 0
        except PyMongoError as e:
            logger.error(f"Database error while updating user: {e}")
            return False
