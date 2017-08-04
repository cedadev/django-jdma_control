from django.views.generic import View
from jdma_control.models import *
from views_functions import *
from datetime import datetime

class UserView(View):
    """:rest-api

    Requests to resources which concern the users in the JASMIN data migration app (JDMA).
    """

    def get(self, request, *args, **kwargs):
        """:rest-api

           .. http:get:: /jdma_control/api/v1/user

               Get the details of a user identified by their username

               :queryparam string name: (*optional*) The username (same as JASMIN username).

               ..

               :>jsonarr string name: username - should be same as JASMIN username
               :>jsonarr string email: email address of the user

               :statuscode 200: request completed successfully.

               **Example request**

               .. sourcecode:: http

                   GET /jdma_control/api/v1/user?name=fred HTTP/1.1
                   Host:jdma.ceda.ac.uk
                   Accept: application/json

               **Example response**

               .. sourcecode:: http

                   HTTP/1.1 200 OK
                   Vary: Accept
                   Content-Type: application/json

                   [
                     {
                       "name": "fred",
                       "email": "fred@fredco.com",
                       "notify": "True"
                     }
                   ]

        """
        # first case - error as you can only retrieve single users
        if len(request.GET) == 0:
            return HttpError({"error": "No name supplied"})
        else:
            # get the username
            username = request.GET.get("name", "")
            # get the user or 404
            error_data = data
            try:
                if username:
                    user = User.objects.get(name=username)
                else:
                    error_data["error"] = "Error with name parameter."
                    return HttpError(data)
            except:
                error_data["error"] = "User not found."
                return HttpError(data)

        data = {"name" : user.name,
                "email" : user.email,
                "notify" : user.notify}

        return HttpResponse(json.dumps(data), content_type="application/json")


    def post(self, request):
        """:rest-api

           .. http:post:: /jdma_control/api/v1/user

                Create a user identified by their username and email address

                ..

                :<jsonarr string name: the username (same as JASMIN username)
                :<jsonarr string email: email address of the user for deletion notifications

                :>jsonarr string name: the username
                :>jsonarr string email: the user's email address

                :statuscode 200: request completed successfully
                :statuscode 403: User already initialized
                :statuscode 404: name not supplied in POST request

                **Example request**

                .. sourcecode:: http

                    POST /jdma_control/api/v1/user HTTP/1.1
                    Host: jdma.ceda.ac.uk
                    Accept: application/json
                    Content-Type: application/json

                    [
                      {
                        "name": "fred",
                        "email": "fred@fredco.com",
                      }
                    ]


                **Example response**

                .. sourcecode:: http

                    HTTP/1.1 200 OK
                    Vary: Accept
                    Content-Type: application/json

                    [
                      {
                        "name": "fred",
                        "email": "fred@fredco.com",
                      }
                    ]

                .. sourcecode:: http

                    HTTP/1.1 403 Forbidden
                    Vary: Accept
                    Content-Type: application/json

                    [
                      {
                        "error": "JASMIN data migration app already initialized for this user"
                      }
                    ]

                .. sourcecode:: http

                    HTTP/1.1 400 Not found
                    Vary: Accept
                    Content-Type: application/json

                    [
                      "No username supplied to POST request"
                    ]

                    [
                      "No email supplied to POST request"
                    ]
        """
        # get the json formatted data
        data = request.read()
        data = json.loads(data)
        # copy data into error data
        error_data = data

        # get the user name and email out of the json
        if "name" in data:
            username = data["name"]
        else:
            return HttpError({"error": "No name supplied"})

        if "email" in data:
            email = data["email"]
        else:
            error_data["error"] = "No email supplied"
            return HttpError(error_data)

        # check if user already exists
        user_query = User.objects.filter(name=username)
        if len(user_query) != 0:
            error_data["error"] = "JDMA already initialized for this user."
            return HttpError(error_data, status=403)
        # create user object
        user = User(name = username, email=email)
        user.save()
        # return the details
        data_out = {"name" : username, "email" : email}
        return HttpResponse(json.dumps(data_out), content_type="application/json")


    def put(self, request, *args, **kwargs):
        """:rest-api

        .. http:put:: /jdma_control/api/v1/user

            Update a user info - just allows update of email address and notifications at the moment

            ..

            :queryparam string name: The username (same as JASMIN username).

            :<jsonarr string email: (*optional*) email address of the user for deletion notifications
            :<jsonarr boolean notify: (*optional*) whether to email the user about scheduled deletions

            :>jsonarr string name: the username
            :>jsonarr string email: the user's email address
            :>jsonarr bool notify: notifications on / off

            :statuscode 200: request completed successfully
            :statuscode 404: name not supplied in PUT request
            :statuscode 404: name not found as supplied in PUT request

            **Example request**

            .. sourcecode:: http

                PUT /jdma_control/api/v1/user HTTP/1.1
                Host: jdma.ceda.ac.uk
                Accept: application/json
                Content-Type: application/json

                [
                  {
                    "name": "fred",
                    "email": "fred@fredco.com",
                    "notify": true,
                  }
                ]


            **Example response**

            .. sourcecode:: http

                HTTP/1.1 200 OK
                Vary: Accept
                Content-Type: application/json

                [
                  {
                    "name": "fred",
                    "email": "fred@fredco.com",
                    "notify": true,
                  }
                ]

            .. sourcecode:: http

                HTTP/1.1 404 Not found
                Vary: Accept
                Content-Type: application/json

                [
                  "No username supplied to PUT request"
                ]

                [
                  "User not found as supplied in PUT request"
                ]
        """
        # find the user first
        if len(request.GET) == 0:
            return HttpError({"error": "No name supplied."})
        else:
            # get the username
            username = request.GET.get("name", "")
            # copy the data into error_data
            error_data = data
            try:
                if username:
                    user = User.objects.get(name=username)
                else:
                    error_data["error"] = "Error with name parameter."
                    return HttpError(error_data)
            except:
                error_data["error"] = "User not found."
                return HttpError(error_data)
            # update the user using the json
            data = request.read()
            data = json.loads(data)
            if "email" in data:
                email = data["email"]
                if email == "":
                    error_data["error"] = "No email supplied."
                    return HttpError(error_data)
                else:
                    user.email = data["email"]

            if "notify" in data:
                user.notify = data["notify"]
            else:
                data["notify"] = user.notify
            user.save()
            # return something meaningful
            data_out = {"name": user.name, "email": user.email, "notify": user.notify}
            return HttpResponse(json.dumps(data_out), content_type="application/json")


class MigrationRequestView(View):
    """:rest-api

    Requests to resources concerning migration requests in the JASMIN data migration app (JDMA).
    """

    def get(self, request, *args, **kwargs):
        """:rest-api

           .. http:get:: /jdma_control/api/v1/migration

               Get the details of one or more migrations identified by the request_id and username,
                 and optionally filtered by the workspace.
               If the request_id is not supplied then all of the requests for that user / workspace
                 are returned

               :param string name: The name of the user who owns the migration request.
               :param string workspace: (*optional*) The workspace that the migration request belongs to.
               :param string request_id: (*optional*) The unique id of the migration request.

               ..

               :>jsonarr string name: username - should be same as JASMIN username
               :>jsonarr string workspace: group workspace name - should be the same as the GWS
               :>jsonarr string label: human readable label of the request
               :>jsonarr string original_path: path of the original directory
               :>jsonarr integer et_id: the elastic tape batch id

               :statuscode 200: request completed successfully.

               **Example request**

               .. sourcecode:: http

                   GET /jdma_control/api/v1/migration/225 HTTP/1.1
                   Host:jdma.ceda.ac.uk
                   Accept: application/json

               **Example response**

               .. sourcecode:: http

                   HTTP/1.1 200 OK
                   Vary: Accept
                   Content-Type: application/json

                   [
                     {
                       "name": "fred",
                       "email": "fred@fredco.com",
                       "notify": "True"
                     }
                   ]
        """
        # if the user name isn't in the request then reject
        if not "name" in request.GET:
            return HttpError({"error" : "No name supplied."})

        # return details of a single request
        if "name" in request.GET and "request_id" in request.GET:
            # get the keywords
            keyargs = {"pk": int(request.GET.get("request_id")),
                       "user__name": request.GET.get("name")}
            if "workspace" in request.GET:
                keyargs["workspace"] = request.GET.get("workspace")

            try:
                req = MigrationRequest.objects.get(**keyargs)
            except:
                # return error as easily interpreted JSON
                error_data = {"error"  : "Migration request not found.",
                              "request_id" : keyargs["pk"],
                              "name"   : keyargs["user__name"]}
                if "workspace" in keyargs:
                    error_data["workspace"] = keyargs["workspace"]
                return HttpError(error_data)

            # full details - these are all the required fields
            data = {"request_id":req.id, "user":req.user.name, "workspace":req.workspace,
                    "request_type":req.request_type, "stage" : req.stage}
            # now add the optional fields
            if req.et_id:
                data["et_id"] = req.et_id
            if req.label:
                data["label"] = req.label
            if req.original_path:
                data["original_path"] = req.original_path
            if req.registered_date:
                data["registered_date"] = req.registered_date.isoformat()
            if req.unix_user_id:
                data["unix_user_id"] = req.unix_user_id
            if req.unix_group_id:
                data["unix_group_id"] = req.unix_group_id
            if req.unix_permission:
                data["unix_permission"] = req.unix_permission
        else:
            # return details of all the migration requests for this user
            keyargs = {"user__name": request.GET.get("name")}
            if "workspace" in request.GET:
                keyargs["workspace"] = request.GET.get("workspace")

            try:
                reqs = MigrationRequest.objects.filter(**keyargs)
            except:
                # return error as easily interpreted JSON
                error_data = {"error"  : "Migration request not found.",
                              "name"   : keyargs["user__name"]}
                if "workspace" in keyargs:
                    error_data["workspace"] = keyargs["workspace"]
                return HttpError(error_data)
            # loop over the requests and add to the data at the end
            requests = []
            for r in reqs:
                req_data = {"request_id": r.pk, "user":r.user.name, "workspace":r.workspace,
                            "request_type":r.request_type, "stage" : r.stage}
                if r.registered_date:
                    req_data["registered_date"] = r.registered_date.isoformat()
                if r.label:
                    req_data["label"] = r.label
                requests.append(req_data)
            data = {"requests": requests}
        return HttpResponse(json.dumps(data), content_type="application/json")


    def post(self, request, *args, **kwargs):
        """:rest-api

           .. http:post:: /jdma_control/api/v1/migration

               Make a request for a migration to the JDMA.

               ..

               :<jsonarr string name: the user id to use in making the request
               :<jsonarr string workspace: the workspace to use in making the request
               :<jsonarr string request_type: GET | PUT | VERIFY
               :<jsonarr string original_path: the path of the original directory
               :<jsonarr string label: (*optional*) a human readable label for the request.
                 A default will be derived from the original path if no label is supplied in the POST request.
        """
        # first do some error checking on the request and get the values if the keywords are in the request
        # check name is in request
        # get the json formatted data
        data = request.read()
        data = json.loads(data)
        # copy data to error_data
        error_data = data

        if not "name" in data:
            error_data["error"] = "No name supplied."
            return HttpError(error_data)

        # check name exists as a user
        try:
            user = User.objects.get(name=data["name"])
        except:
            error_data["error"] = "User not found."
            return HttpError(data)

        # check workspace is in request
        if not "workspace" in data:
            error_data["error"] = "No workspace supplied."
            return HttpError(error_data)

        # check workspace exists?

        # check request type is in request
        if not "request_type" in data:
            error_data["error"] = "No request type supplied."
            return HttpError(error_data)

        # check request type is "GET", "PUT" or "VERIFY"
        if not data["request_type"] in ["GET", "PUT", "VERIFY"]:
            error_data["error"] = "Invalid request method."
            return HttpError(error_data)

        # check original path is in the request
        if not "original_path" in data:
            error_data["error"] = "No directory path supplied."
            return HttpError(error_data)
        else:
            original_path = data["original_path"]
            # remove any trailing slash
            if original_path[-1] == "/":
                original_path = original_path[:-1]

            # check that there is not already an entry with this path
            if MigrationRequest.objects.filter(original_path=original_path):
                error_data["error"] = "Directory is already in a migration request."
                return HttpError(error_data)

        # check for the label in the request - if not then derive from directory name
        if "label" in data:
            label = data["label"]
        else:
            label = original_path.split("/")[-1]

        # three checks:
        #   1. Check the path exists (obvs.)
        #   2. Check the user has write permission to the directory
        #   3. Check the user has enough space in their ET quota

        # 1. check that the path exists
        if not os.path.isdir(original_path):
            error_data["error"] = "Directory path does not exist."
            return HttpError(error_data)

        # 2. check that the user has write permissions
        if not UserHasWritePermission(original_path, data["name"]):
            error_data["error"] = "User does not have write permission to the directory."
            return HttpError(error_data)

        # 3. check et_quota ** TO DO ** implement this function!
        if not UserHasETQuota(original_path, data["name"], data["workspace"]):
            error_data["error"] = "Insufficient remaining Elastic Tape quota."
            return HttpError(error_data)

        # All the checks have passed, so we can now add the request to the JDMA database
        migration_request = MigrationRequest()
        # first assign the data passed in / derived above
        migration_request.user = User.objects.get(name=data["name"])
        migration_request.label = label
        migration_request.workspace = data["workspace"]
        migration_request.request_type = MigrationRequest.REQUEST_MAP[data["request_type"]]

        # Assign the stage, this is always on disk at this stage
        migration_request.stage = MigrationRequest.ON_DISK

        # get the date
        migration_request.registered_date = datetime.utcnow()

        fstat = os.stat(original_path)

        # get the unix user id owner of the file
        migration_request.unix_user_id = pwd.getpwuid(fstat.st_uid).pw_name

        # get the unix group id owner of the file
        migration_request.unix_group_id = grp.getgrgid(fstat.st_gid).gr_name

        # get the unix permissions
        migration_request.unix_permission = oct(fstat.st_mode & 0777)[1:]

        # path
        migration_request.original_path = original_path

        # save to the database
        migration_request.save()

        # build the return data
        return_data = data
        return_data["request_id"] = migration_request.pk
        return_data["request_type"] = migration_request.request_type
        return_data["stage"] =  migration_request.stage
        return_data["registered_date"] = migration_request.registered_date.isoformat()
        return_data["label"] = migration_request.label
        return_data["unix_user_id"] = migration_request.unix_user_id
        return_data["unix_group_id"] = migration_request.unix_group_id
        return_data["unix_permission"] = migration_request.unix_permission

        return HttpResponse(json.dumps(return_data), content_type="application/json")


    def put(self, request, *args, **kwargs):
        """:rest-api

           .. http:post:: /jdma_control/api/v1/migration

               Modify a request for a migration to the JDMA.

               ..
               :param string name: The name of the user who owns the migration request.
               :param string request_id: (*optional*) The unique id of the migration request.

               :<jsonarr string label: (*optional*) a human readable label for the request.
        """
        # if the user name isn't in the request then reject
        if not "name" in request.GET:
            return HttpError({"error" : "No name supplied."})

        # if the user name isn't in the request then reject
        if not "request_id" in request.GET:
            return HttpError({"error" : "No request id supplied."})

        # read the data
        data = request.read()
        data = json.loads(data)
        # copy data to error_data
        error_data = data

        if not "label" in data:
            error_data["error"] = "No label supplied."
            return HttpError(error_data)

        # try to get the migration request
        req_id = int(request.GET.get("request_id"))
        username = request.GET.get("name")
        try:
            migration_request = MigrationRequest.objects.get(pk=req_id)
        except:
            error_data = {"error": "Migration request not found.",
                          "request_id": req_id,
                          "name": username}
            return HttpError(error_data)

        # check that the migration request belongs to this user
        if username != migration_request.user.name:
            error_data = {"error": "User cannot edit this migration request as they do not own it.",
                          "request_id": req_id,
                          "name": username}
            return HttpError(error_data)

        # otherwise modify it
        migration_request.label = data["label"]
        migration_request.save()

        return HttpResponse(json.dumps({"none":"none"}), content_type="application/json")
