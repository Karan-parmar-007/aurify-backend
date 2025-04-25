from flask import request, jsonify
from app.blueprints.auth import auth_bp
from app.models.user_model import UserModel
from app.utils.logger import logger

user_model = UserModel()

@auth_bp.route('/verify_user', methods=['POST'])
def verify_user():
    """Verify user credentials
    
    Request body:
    {
        "email": "user@example.com",
        "password": "password123"
    }
    
    Returns:
        JSON response with status and user_id if successful
    """
    try:
        data = request.get_json()
        
        # Validate required fields
        required_fields = ['email', 'password']
        for field in required_fields:
            if field not in data:
                return jsonify({
                    'status': 'error',
                    'message': f'Missing required field: {field}'
                }), 400
        
        # Verify user credentials
        is_valid, user_id = user_model.verify_user(
            email=data['email'],
            password=data['password']
        )
        
        if is_valid:
            return jsonify({
                'status': 'success',
                'message': 'User verified successfully',
                'user_id': user_id
            }), 200
        else:
            return jsonify({
                'status': 'error',
                'message': 'Invalid email or password'
            }), 401
            
    except Exception as e:
        logger.error(f"Error in verify_user: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred'
        }), 500