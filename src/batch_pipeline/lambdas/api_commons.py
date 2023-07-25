import json
def form_response(body, code = 200):
    
    response = {
        "isBase64Encoded": False,
        "statusCode": code, 
        "body": json.dumps(body)
    }
    return response

def handle_exception(exception: Exception, code = 500):
    message = None
    if hasattr(exception, 'message'):
        message = message
    else:
        message = str(exception)
    body = {
        "error": message
    }
    return form_response(body, code)
