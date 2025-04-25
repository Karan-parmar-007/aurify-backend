from flask import request, jsonify
from app.blueprints.admin import admin_bp
from app.models.system_column_model import SystemColumnModel
from app.utils.logger import logger

system_column_model = SystemColumnModel()

@admin_bp.route('/get_system_columns', methods=['GET'])
def get_all_columns():
    """Get all system columns
    
    Returns:
        JSON response with list of all system columns
    """
    try:
        columns = system_column_model.get_all_columns()
        if columns is not None:
            return jsonify({
                'status': 'success',
                'data': columns
            }), 200
        else:
            return jsonify({
                'status': 'error',
                'message': 'Failed to fetch system columns'
            }), 500
    except Exception as e:
        logger.error(f"Error in get_all_columns: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred'
        }), 500

@admin_bp.route('/add_system_columns', methods=['POST'])
def add_column():
    """Add a new system column
    
    Request body:
    {
        "column_name": "Column Name",
        "description": "Column Description",
        "alt_names": ["alt1", "alt2"],
        "asset_class": "Asset Class"
    }
    
    Returns:
        JSON response with status and column ID if successful
    """
    try:
        data = request.get_json()
        
        # Validate required fields
        required_fields = ['column_name', 'description', 'alt_names', 'asset_class']
        for field in required_fields:
            if field not in data:
                return jsonify({
                    'status': 'error',
                    'message': f'Missing required field: {field}'
                }), 400
        
        # Create new column
        column_id = system_column_model.create_column(
            column_name=data['column_name'],
            description=data['description'],
            alt_names=data['alt_names'],
            asset_class=data['asset_class']
        )
        
        if column_id:
            return jsonify({
                'status': 'success',
                'message': 'Column created successfully',
                'column_id': column_id
            }), 201
        else:
            return jsonify({
                'status': 'error',
                'message': 'Failed to create column'
            }), 500
            
    except Exception as e:
        logger.error(f"Error in add_column: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred'
        }), 500

@admin_bp.route('/update_system_columns/<column_id>', methods=['PUT'])
def update_column(column_id):
    """Update a system column
    
    Args:
        column_id (str): ID of the column to update
        
    Request body:
    {
        "column_name": "Updated Name",
        "description": "Updated Description",
        "alt_names": ["new_alt1", "new_alt2"],
        "asset_class": "Updated Asset Class"
    }
    
    Returns:
        JSON response with status
    """
    try:
        data = request.get_json()
        
        # Validate required fields
        required_fields = ['column_name', 'description', 'alt_names', 'asset_class']
        for field in required_fields:
            if field not in data:
                return jsonify({
                    'status': 'error',
                    'message': f'Missing required field: {field}'
                }), 400
        
        # Update column
        success = system_column_model.update_column(column_id, data)
        
        if success:
            return jsonify({
                'status': 'success',
                'message': 'Column updated successfully'
            }), 200
        else:
            return jsonify({
                'status': 'error',
                'message': 'Failed to update column'
            }), 500
            
    except Exception as e:
        logger.error(f"Error in update_column: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred'
        }), 500

@admin_bp.route('/delete_system_columns/<column_id>', methods=['DELETE'])
def delete_column(column_id):
    """Delete a system column
    
    Args:
        column_id (str): ID of the column to delete
        
    Returns:
        JSON response with status
    """
    try:
        success = system_column_model.delete_column(column_id)
        
        if success:
            return jsonify({
                'status': 'success',
                'message': 'Column deleted successfully'
            }), 200
        else:
            return jsonify({
                'status': 'error',
                'message': 'Failed to delete column'
            }), 500
            
    except Exception as e:
        logger.error(f"Error in delete_column: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred'
        }), 500 