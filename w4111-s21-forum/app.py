"""
Copyright @Donald F. Ferguson, 2021

This file is part of the application template for W4111, Section 002, Spring 21 HW assignments 3 and 4.

app.py is the 'main program.'

"""
import json
# DFF TODO -- Not critical for W4111, but should switch from print statements to logging framework.
import logging
from datetime import datetime

#
# These packages provide functions for deliverying a web application using Flask.
# Students can look online for education resources.
#
from flask import Flask, Response
from flask import Flask, request, render_template, jsonify

# rest_utils provides simplification and isolation for interacting with Flask APIs and object,
# specifically the request object.
import utils.rest_utils as rest_utils

#
# DFF TODO -- Importing the service classes in the main app.py is easy but not a best practice
# The class implements one of the resources this application exposes.
#

from Services.ForumsService.ForumService import ForumsService


# DFF TODO - We should not hardcode this here, and we should do in a context/environment service.
# OK for W4111 - This is not a course on microservices and robust programming.
#
# DFF TODO -- This needs to be cleaned up over time.
# The key in the dict is the exposed resource name. The value is the implementation.
# See the class implementation to understand the constructor parameters.
#
c_info = {
    "db_connect_info": {
        "HOST": "localhost",
        "PORT": 27017,
        "DB": "forums"
    }
}
# dbUser/ygFc8!8vrRELuKf
# mongodb+srv://dbUser:<password>@cluster0.zozha.mongodb.net/myFirstDatabase?retryWrites=true&w=majority
_service_factory = {
    "forums": ForumsService("forum", c_info, key_columns=["post_id"])
}


#
# Given the "resource" return the implementing class.
def _get_service_by_name(s_name):
    result = _service_factory.get(s_name, None)
    return result

#
# Create the Flask application object.
app = Flask(__name__)


"""
Import route definitions
"""


##################################################################################################################

# DFF TODO A real service would have more robust health check methods.
# This path simply echoes to check that the app is working.
# The path is /health and the only method is GETs
@app.route("/health", methods=["GET"])
def health_check():
    rsp_data = {"status": "healthy", "time": str(datetime.now())}
    rsp_str = json.dumps(rsp_data)
    rsp = Response(rsp_str, status=200, content_type="app/json")
    return rsp


# TODO Remove later. Solely for explanatory purposes.
# The method take any REST request, and produces a response indicating what
# the parameters, headers, etc. are. This is simply for education purposes.
#
@app.route("/api/demo/<parameter1>", methods=["GET", "POST", "PUT", "DELETE"])
@app.route("/api/demo/", methods=["GET", "POST", "PUT", "DELETE"])
def demo(parameter1=None):
    """
    Returns a JSON object containing a description of the received request.

    :param parameter1: The first path parameter.
    :return: JSON document containing information about the request.
    """

    # DFF TODO -- We should wrap with an exception pattern.
    #

    # Mostly for isolation. The rest of the method is isolated from the specifics of Flask.
    inputs = rest_utils.RESTContext(request, {"parameter1": parameter1})

    # DFF TODO -- We should replace with logging.
    r_json = inputs.to_json()
    msg = {
        "/demo received the following inputs": inputs.to_json()
    }
    print("/api/demo/<parameter> received/returned:\n", msg)

    rsp = Response(json.dumps(msg), status=200, content_type="application/json")
    return rsp


# /api/forum?emai=asample1l@privacy.gov.au&children.tags=HW4"
@app.route("/api/forums", methods=["POST"])
def post_template():

    try:
        # DFF TODO Change this to a DTO/object with properties from a dict.
        inputs = rest_utils.RESTContext(request, {"parameter1": None})
        rest_utils.log_request("post_template", inputs)
        service = _get_service_by_name("forums")

        if service is not None:
            rsp = service.insert_forum(inputs.data)
            rsp = json.dumps(rsp, default=str)
            rsp = Response(rsp, status=200, content_type="application/JSON")
        else:
            rsp = Response("NOT FOUND", status=404, content_type="text/plain")

    except Exception as e:
        # TODO Put a common handler to catch excceptions, log the error and return correct
        # HTTP status code.
        print("/api/" + "some weird path" + "/count, e = ", e)
        rsp = Response("INTERNAL ERROR", status=500, content_type="text/plain")

    return rsp

# /api/forum?emai=asample1l@privacy.gov.au&children.tags=HW4"
@app.route("/api/forums", methods=["GET"])
def get_template():

    try:
        # DFF TODO Change this to a DTO/object with properties from a dict.
        inputs = rest_utils.RESTContext(request, {"parameter1": None})
        rest_utils.log_request("get_template", inputs)

        service = _get_service_by_name("forums")

        if service is not None:
            rsp = service.find_by_template(inputs.args)
            rsp = json.dumps(rsp, default=str)
            rsp = Response(rsp, status=200, content_type="application/JSON")
        else:
            rsp = Response("NOT FOUND", status=404, content_type="text/plain")

    except Exception as e:
        # TODO Put a common handler to catch excceptions, log the error and return correct
        # HTTP status code.
        print("/api/" + "some weird path" + "/count, e = ", e)
        rsp = Response("INTERNAL ERROR", status=500, content_type="text/plain")

    return rsp


##################################################################################################################
# Actual routes begin here.
#
#
@app.route("/api/forums/<post_id>", methods=["GET"])
def get_post_by_id(post_id):

    try:
        # DFF TODO Change this to a DTO/object with properties from a dict.
        inputs = rest_utils.RESTContext(request, {"parameter1": None})
        rest_utils.log_request("get_post_by_id", inputs)

        service = _get_service_by_name("forums")

        if service is not None:
            rsp = service.find_by_template({"post_id": post_id})
            rsp = json.dumps(rsp, default=str)
            rsp = Response(rsp, status=200, content_type="application/JSON")
        else:
            rsp = Response("NOT FOUND", status=404, content_type="text/plain")

    except Exception as e:
        # TODO Put a common handler to catch excceptions, log the error and return correct
        # HTTP status code.
        print("/api/" + "some weird path" + "/count, e = ", e)
        rsp = Response("INTERNAL ERROR", status=500, content_type="text/plain")

    return rsp


@app.route("/api/forums/<post_id>/comments", methods=["GET"])
def get_get_by_id_comments(post_id):

    try:
        # DFF TODO Change this to a DTO/object with properties from a dict.
        inputs = rest_utils.RESTContext(request, {"parameter1": None})
        rest_utils.log_request("get_get_by_id_comments", inputs)

        service = _get_service_by_name("forums")

        if service is not None:
            rsp = service.find_by_template({"post_id": post_id, **inputs.args})
            if len(rsp) == 1:
                rsp = json.dumps(rsp, default=str)
                rsp = Response(rsp, status=200, content_type="application/JSON")
            else:
                rsp = Response("NOT FOUND", status=404, content_type="text/plain")
        else:
            rsp = Response("NOT FOUND", status=404, content_type="text/plain")

    except Exception as e:
        # TODO Put a common handler to catch excceptions, log the error and return correct
        # HTTP status code.
        print("/api/" + "some weird path" + "/count, e = ", e)
        rsp = Response("INTERNAL ERROR", status=500, content_type="text/plain")

    return rsp


@app.route("/api/forums/<post_id>/comments", methods=["POST"])
def get_post_by_id_comments(post_id):

    try:
        # DFF TODO Change this to a DTO/object with properties from a dict.
        inputs = rest_utils.RESTContext(request, {"parameter1": None})
        rest_utils.log_request("get_post_by_id_comments", inputs)

        service = _get_service_by_name("forums")

        if service is not None:
            rsp = service.find_by_template({"post_id": post_id})
            if rsp and len(rsp) == 1:
                rsp = service.insert_comment(rsp[0]['post_id'], inputs.data)
                rsp = json.dumps(rsp, default=str)
                rsp = Response(rsp, status=200, content_type="application/JSON")
            else:
                rsp = Response("NOT FOUND", status=404, content_type="text/plain")
        else:
            rsp = Response("NOT FOUND", status=404, content_type="text/plain")

    except Exception as e:
        # TODO Put a common handler to catch excceptions, log the error and return correct
        # HTTP status code.
        print("/api/" + "some weird path" + "/count, e = ", e)
        rsp = Response("INTERNAL ERROR", status=500, content_type="text/plain")

    return rsp



@app.route("/api/forums/<post_id>/comments/<comment_id>", methods=["GET"])
def get_get_by_id_comment_id(post_id, comment_id):

    try:
        # DFF TODO Change this to a DTO/object with properties from a dict.
        inputs = rest_utils.RESTContext(request, {"parameter1": None})
        rest_utils.log_request("get_get_by_id_comment_id", inputs)

        service = _get_service_by_name("forums")

        if service is not None:
            rsp = service.find_by_template({"post_id": post_id})
            if len(rsp) == 1:
                res = None
                for child in rsp[0]['children']:
                    if child['comment_id'] == comment_id:
                        res = child
                        break

                rsp = json.dumps(res, default=str)
                rsp = Response(rsp, status=200, content_type="application/JSON")
            else:
                rsp = Response("NOT FOUND", status=404, content_type="text/plain")
        else:
            rsp = Response("NOT FOUND", status=404, content_type="text/plain")

    except Exception as e:
        # TODO Put a common handler to catch excceptions, log the error and return correct
        # HTTP status code.
        print("/api/" + "some weird path" + "/count, e = ", e)
        rsp = Response("INTERNAL ERROR", status=500, content_type="text/plain")

    return rsp


if __name__ == '__main__':
    #host, port = ctx.get_host_and_port()

    # DFF TODO We will handle host and SSL certs different in deployments.
    app.run(host="0.0.0.0", port=5001)
