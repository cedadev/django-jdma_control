from django.views.generic import View
from jdma_control.models import *
from jdma_control.views_functions import *
from datetime import datetime
import subprocess

from jasmin_ldap.core import *
from jasmin_ldap.query import *

import jdma_site.settings as settings
import jdma_control.backends

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
            error_data = {}
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
    Note - no PUT, cannot edit any part of the request
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
               :>jsonarr integer external_id: the elastic tape batch id

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

            try:
                req = MigrationRequest.objects.get(**keyargs)
            except:
                # return error as easily interpreted JSON
                error_data = {"error"  : "Request not found.",
                              "request_id" : keyargs["pk"],
                              "name"   : keyargs["user__name"]}
                return HttpError(error_data)

            # determine the stage
            if req.request_type == MigrationRequest.PUT:
                migration_stage = req.migration.stage
            elif req.request_type == MigrationRequest.GET:
                migration_stage = req.stage + 10            # plus 10 to indicate GET

            # full details - these are all the required fields
            data = {"request_id": req.id, "user": req.user.name,
                    "request_type": req.request_type,
                    "migration_id": req.migration.pk,
                    "migration_label": req.migration.label,
                    "workspace": req.migration.workspace,
                    "stage": migration_stage}
            if req.date:
                data["date"] = req.date.isoformat()
        else:
            # return details of all the migration requests for this user
            keyargs = {"user__name": request.GET.get("name")}
            try:
                reqs = MigrationRequest.objects.filter(**keyargs)
            except:
                # return error as easily interpreted JSON
                error_data = {"error"  : "Request not found.",
                              "name"   : keyargs["user__name"]}
                return HttpError(error_data)

            # loop over the requests and add to the data at the end
            requests = []
            for r in reqs:
                # determine the stage
                if r.request_type == MigrationRequest.PUT:
                    migration_stage = r.migration.stage
                elif r.request_type == MigrationRequest.GET:
                    migration_stage = r.stage + 10

                req_data = {"request_id": r.pk, "user": r.user.name,
                            "request_type": r.request_type,
                            "migration_id": r.migration.pk,
                            "migration_label": r.migration.label,
                            "workspace": r.migration.workspace,
                            "stage": migration_stage}
                if r.date:
                    req_data["date"] = r.date.isoformat()
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
               :<jsonarr string original_path: the path of the original directory (for PUT)
               :<jsonarr string label: (*optional*) a human readable label for the request (for PUT or GET).
                 A default will be derived from the original path if no label is supplied in the POST request.
               :<jsonarr int id: the id of the Migration to retrieve (for GET)
               :<jsonarr string
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
            return HttpError(error_data, status=500)

        # check name exists as a user
        try:
            user = User.objects.get(name=data["name"])
        except:
            error_data["error"] = "User not found."
            return HttpError(data)

        # check request type is in request
        if not "request_type" in data:
            error_data["error"] = "No request type supplied."
            return HttpError(error_data)

        # check request type is "GET", "PUT"
        if not data["request_type"] in ["GET", "PUT"]:
            error_data["error"] = "Invalid request method."
            return HttpError(error_data)

        # create the MigrationRequest (GET, PUT)
        migration_request = MigrationRequest()
        # assign the user to the MigrationRequest
        migration_request.user = user
        # get the migration request type
        migration_request.request_type = MigrationRequest.REQUEST_MAP[data["request_type"]]
        # get the date
        cdate = datetime.utcnow()
        # set the date
        migration_request.date = cdate

        # create the LDAP server pool needed in both GET and PUT requests
        ldap_servers = ServerPool(settings.JDMA_LDAP_PRIMARY, settings.JDMA_LDAP_REPLICAS)

        # now choose what to do based on the request
        if data["request_type"] == "GET":
            # checks
            #   1. Check that the request id is supplied
            #   2. Check that the request exists
            #   3. Check that the request belongs to the user, or has group or universal permission
            #   4. Check that the stage is ON_TAPE
            #   5. Check the user has permission to write to the target directory (or original path if not set)
            #   6. Check whether this is a duplicate
            #   7. Check that the target path exists
            #   8. Check that the user has permission to write to the target directory
            #   9. Check there is enough space on disk

            #   1. check request id is supplied
            if not "migration_id" in data:
                error_data["error"] = "No batch id supplied."
                return HttpError(error_data)

            #   2. check request id exists
            try:
                mig = Migration.objects.get(pk=data["migration_id"])
            except:
                error_data["error"] = "Batch not found."
                return HttpError(error_data)

            #   3. check that the migration belongs to the user, or has group or universal permission
            if mig.permission == Migration.PERMISSION_PRIVATE:
                if mig.user.name != user.name:
                    error_data["error"] = "User " + user.name + " does not have permission to request the batch."
                    return HttpError(error_data)

            if mig.permission == Migration.PERMISSION_GROUP:
                # check that the user is in a group that matches the workspace
                # get the users in the workspace group
                with Connection.create(ldap_servers) as conn:
                    query = Query(conn, base_dn=settings.JDMA_LDAP_BASE_GROUP).filter(cn=mig.workspace)

                    # check for a valid return
                    if len(query) == 0:
                        logging.error("Group workspace: {} not found from LDAP".format(mig.workspace))

                    if len(query) == 0 or not user.name in query[0]['memberUid']:
                        error_data["error"] = "User " + user.name + " does not have permission to request the batch."
                        return HttpError(error_data)

            #   4. check that the stage is ON_TAPE
            if mig.stage != Migration.ON_TAPE:
                mig_stage = Migration.STAGE_CHOICES[mig.stage][1]
                error_data["error"] = "Batch stage is: " + mig_stage + ".  Cannot retrieve (GET) until stage is ON_TAPE."
                return HttpError(error_data)

            # We don't need to create a migration as we're operating on an existing one - assign it
            migration_request.migration = mig

            #   5. check the user has permission to write to the target directory (or original path if not set)
            # get the target dir
            if "target_path" in data:
                target_path = data["target_path"]
            else:
                target_path = migration_request.migration.original_path

            #   6. check if this is a duplicate
            dup_req = MigrationRequest.objects.filter(migration=mig, target_path=target_path)
            if len(dup_req) != 0:
                error_data["error"] = "Duplicate GET request made: Batch ID: {}, Target path: {}".format(mig.external_id, target_path)
                return HttpError(error_data, status=403)

            #   7. check the target path exists
            base_path = os.path.dirname(target_path)
            if not os.path.exists(base_path):
                error_data["error"] = "Parent of target path does not exist: " + str(base_path)
                return HttpError(error_data, status=403)

            #   8. check the user has permission to write to the directory
            if not user_has_write_permission(base_path, data["name"]):
                error_data["error"] = "User " + data["name"] + " does not have write permission to the directory: " + str(target_path)
                return HttpError(error_data, status=403)

            #   9. Check there is enough space on disk
            retrieval_size = 0
            if not user_has_sufficient_diskspace(base_path, data["name"], retrieval_size): # implement this function
                error_data["error"] = "Insufficient diskspace for the retrieval (GET) " + str(target_path)
                return HttpError(error_data, status=403)

            # All the checks have been passed so we can now add the request to the JDMA database
            migration_request.target_path = target_path
            migration_request.stage = MigrationRequest.ON_TAPE

            migration_request.save()
            # build the return data
            return_data = data
            return_data["request_id"] = migration_request.pk
            return_data["batch_id"] = migration_request.migration.external_id
            return_data["request_type"] = migration_request.request_type
            return_data["workspace"] = migration_request.migration.workspace
            return_data["stage"] =  migration_request.migration.stage
            return_data["registered_date"] = migration_request.date.isoformat()
            return_data["label"] = migration_request.migration.label
            return_data["target_path"] = target_path

        elif data["request_type"] == "PUT":
            # check workspace is in request
            if not "workspace" in data:
                error_data["error"] = "No workspace supplied."
                return HttpError(error_data)

            # check workspace exists - get the group for the workspace from LDAP
            with Connection.create(ldap_servers) as conn:
                query = Query(conn, base_dn=settings.JDMA_LDAP_BASE_GROUP).filter(cn=data["workspace"])

                # check for a valid return
                if len(query) == 0:
                    error_data["error"] = "Workspace " + data["workspace"] + " does not exist."
                    return HttpError(error_data)

                # check that user is in this workspace
                if not user.name in query[0]['memberUid']:
                    error_data["error"] = "User " + user.name + " does not belong to the group workspace " + data["workspace"]+"."
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
                if Migration.objects.filter(original_path=original_path):
                    error_data["error"] = "Directory is already in a migration."
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
            if not user_has_write_permission(original_path, data["name"]):
                error_data["error"] = "User does not have write permission to the directory " + original_path
                return HttpError(error_data, status=403)

            # 3. check et_quota ** TO DO ** implement this function!
            if not settings.JDMA_BACKEND_OBJECT.user_has_remaining_quota(original_path, data["name"], data["workspace"]):
                error_data["error"] = "Insufficient remaining quota for " + settings.JDMA_BACKEND_OBJECT.get_name()
                return HttpError(error_data, status=403)

            # All the checks have passed, so we can now add the request to the JDMA database
            # Create a Migration (all the details of the directory)
            migration = Migration()
            migration.user = user

            # Assign the data passed in / derived above
            migration.label = label
            migration.workspace = data["workspace"]

            # Assign the stage, this is always on disk at this stage for a PUT
            migration.stage = Migration.ON_DISK

            # get the date
            migration.registered_date = cdate

            # get the permissions etc. of the original file
            fstat = os.stat(original_path)

            # get the unix user id owner of the file - use LDAP now
            ldap_servers = ServerPool(settings.JDMA_LDAP_PRIMARY, settings.JDMA_LDAP_REPLICAS)
            with Connection.create(ldap_servers) as conn:
                # query to find username with uidNumber matching fstat.st_uid
                query = Query(conn, base_dn=settings.JDMA_LDAP_BASE_USER).filter(uidNumber=fstat.st_uid)
                if len(query[0]) == 0:
                    logging.error("uidNumber: {} not found from LDAP".format(fstat.st_uid))
                    migration.unix_user_id = ""
                else:
                    migration.unix_user_id = query[0]["uid"][0]

                # query to find group with gidNumber matching fstat.gid
                query = Query(conn, base_dn=settings.JDMA_LDAP_BASE_GROUP).filter(gidNumber=fstat.st_gid)
                if len(query[0]) == 0:
                    logging.error("gidNumber: {} not found from LDAP".format(fstat.st_gid))
                    migration.unix_group_id = ""
                else:
                    migration.unix_group_id = query[0]["cn"][0]

            # get the unix permissions
            migration.unix_permission = (fstat.st_mode & 0o777)

            # path
            migration.original_path = original_path

            # save the migration to the database
            migration.save()

            # associate the migration_request with the migration and save to the database
            migration_request.migration = migration
            migration_request.save()

            # build the return data
            return_data = data
            return_data["request_id"] = migration.pk
            return_data["request_type"] = migration_request.request_type
            return_data["stage"] =  migration.stage
            return_data["registered_date"] = migration.registered_date.isoformat()
            return_data["label"] = migration.label
            return_data["unix_user_id"] = migration.unix_user_id
            return_data["unix_group_id"] = migration.unix_group_id
            return_data["unix_permission"] = migration.unix_permission

        return HttpResponse(json.dumps(return_data), content_type="application/json")


class MigrationView(View):
    """:rest-api

    Requests to resources concerning migrations in the JASMIN data migration app (JDMA).
    Note that there is no POST method, as POST to this model is handled by POSTs to MigrationRequestView
    """

    def get(self, request, *args, **kwargs):
        """:rest-api"""
        # if the user name isn't in the request then reject
        if not "name" in request.GET:
            return HttpError({"error" : "No name supplied."})

        # return details of a single request
        if "name" in request.GET and "migration_id" in request.GET:
            # get the keywords
            keyargs = {"pk": int(request.GET.get("migration_id")),
                       "user__name": request.GET.get("name")}
            if "workspace" in request.GET:
                workspace = request.GET.get("workspace")
                keyargs["workspace"] = workspace
            else:
                workspace = None

            try:
                mig = Migration.objects.get(**keyargs)
            except:
                # return error as easily interpreted JSON
                error_data = {"error"  : "Batch not found.",
                              "migration_id" : keyargs["pk"],
                              "name"   : keyargs["user__name"]}
                if workspace:
                    error_data["workspace"] = workspace
                return HttpError(error_data)

            # full details - these are all the required fields
            data = {"migration_id": mig.id, "user": mig.user.name,
                    "workspace": mig.workspace,
                    "label": mig.label,
                    "stage": mig.stage,
                    "permission": mig.permission}
            # add the optional data if it's there
            if mig.external_id:
                data["external_id"] = mig.external_id
            if mig.registered_date:
                data["registered_date"] = mig.registered_date.isoformat()
            #if mig.tags:
            #    data["tags"] = mig.tags
            if mig.original_path:
                data["original_path"] = mig.original_path
            if mig.unix_user_id:
                data["unix_user_id"] = mig.unix_user_id
            if mig.unix_group_id:
                data["unix_group_id"] = mig.unix_group_id
            if mig.unix_permission:
                data["unix_permission"] = mig.unix_permission
        else:
            # return details of all the migrations for this user
            keyargs = {"user__name": request.GET.get("name")}
            if "workspace" in request.GET:
                workspace = request.GET.get("workspace")
                keyargs["workspace"] = workspace
            else:
                workspace = None

            try:
                migs = Migration.objects.filter(**keyargs)
            except:
                # return error as easily interpreted JSON
                error_data = {"error"  : "Batches not found.",
                              "name"   : keyargs["user__name"]}
                if workspace:
                    error_data["workspace"] = workspace

                return HttpError(error_data)
            # loop over the requests and add to the data at the end
            migrations = []
            for m in migs:
                mig_data = {"migration_id": m.pk, "user": m.user.name,
                            "workspace": m.workspace,
                            "label": m.label,
                            "stage": m.stage,
                            "permission": m.permission}
                if m.registered_date:
                    mig_data["registered_date"] = m.registered_date.isoformat()
                migrations.append(mig_data)
            data = {"migrations": migrations}
        return HttpResponse(json.dumps(data), content_type="application/json")


    def put(self, request, *args, **kwargs):
        """:rest-api

           .. http:post:: /jdma_control/api/v1/migration

               Modify a migration within the JDMA.

               :param string name: The name of the user who owns the migration request.
               :param string migration_id:  The unique id of the migration.

               :<jsonarr string label:  a human readable label for the migration.
        """
        # if the user name isn't in the request then reject
        if not "name" in request.GET:
            return HttpError({"error" : "No name supplied."})

        # if the user name isn't in the request then reject
        if not "migration_id" in request.GET:
            return HttpError({"error" : "No batch id supplied."})

        # read the data
        data = request.read()
        data = json.loads(data)
        # copy data to error_data
        error_data = data

        if "label" in data and data["label"] == "":
            error_data["error"] = "No label supplied."
            return HttpError(error_data)

        if "permission" in data and data["permission"] == "":
            error_data["error"] = "No permission supplied."
            return HttpError(error_data)

        if "permission" in data and not data["permission"] in ["ALL", "PRIVATE", "GROUP"]:
            error_data["error"] = "Permission has to be one of ALL | PRIVATE | GROUP"
            return HttpError(error_data)

        # try to get the migration request
        mig_id = int(request.GET.get("migration_id"))
        username = request.GET.get("name")
        try:
            migration = Migration.objects.get(pk=mig_id)
        except:
            error_data = {"error": "Batch not found.",
                          "migration_id": mig_id,
                          "name": username}
            return HttpError(error_data)

        # check that the migration request belongs to this user
        if username != migration.user.name:
            error_data = {"error": "User " + username + " cannot edit this batch as they do not own it!",
                          "request_id": mig_id,
                          "name": username}
            return HttpError(error_data)

        # otherwise modify it
        if "label" in data:
            migration.label = data["label"]

        if "permission" in data:
            if data["permission"] == "ALL":
                migration.permission = Migration.PERMISSION_ALL
            elif data["permission"] == "PRIVATE":
                migration.permission = Migration.PERMISSION_PRIVATE
            elif data["permission"] == "GROUP":
                migration.permission = Migration.PERMISSION_GROUP
        migration.save()

        return HttpResponse(json.dumps({"none":"none"}), content_type="application/json")
