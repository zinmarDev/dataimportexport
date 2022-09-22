def get_response_json(response_data, response_error):
    print("response data : ", response_error, response_data)
    response_json = {"ResponseCode": 1000, "ResponseMessage": "Request was successful.", "Results": str(response_data)}

    if response_error != "":
        response_json = {"ResponseCode": 2018, "ResponseMessage": "Request was fail.", "Results": str(response_error)}
    return response_json
