"""A container class to create and access connections to the various
backends."""
import jdma_control.backends
import jdma_site.settings as settings

class ConnectionPool:
    """A container class to create and access connections to the various
    backends."""
    def __init__(self):
        self.pool = {}

    def __get_connection_id(connection_id,
                            thread_number="",
                            uid="",
                            mode="upload"):
        if thread_number != None:
            new_connection_id = "{}_{}_{}_{}".format(
                str(connection_id),
                str(thread_number),
                str(uid),
                mode
            )
        else:
            new_connection_id = "{}_{}_{}".format(
                str(connection_id),
                str(uid),
                mode
            )

        return new_connection_id


    def find_or_create_connection(self,
                                  backend_object,
                                  mig_req = None,
                                  credentials = None,
                                  mode="upload",
                                  thread_number=None,
                                  uid=""
        ):
        """The connection pool is a dictionary of connections, with a connection
           id as the key:
           { connection_id : connection_object }"""
        backend_id = backend_object.get_id()
        # allow some defaults
        if mig_req is not None:
            connection_number = mig_req.pk
            user_name = mig_req.migration.user.name
            workspace = mig_req.migration.workspace.workspace
        else:
            connection_number = 0
            user_name = "jdma"
            workspace = "jdma"

        connection_id = ConnectionPool.__get_connection_id(
            connection_number,
            thread_number,
            uid,
            mode,
        )
        if backend_id not in self.pool:
            # backend not found - create connection and asssign
            conn = backend_object.create_connection(
                user_name,
                workspace,
                credentials,
                mode
            )
            if settings.TESTING:
                print("Creating new connection_id {}".format(connection_id))
            conn_dict = {connection_id : conn}
            self.pool[backend_id] = conn_dict
        else:
            # search for mig_req.pk in the backend
            if connection_id in self.pool[backend_id]:
                # found
                if settings.TESTING:
                    print("Using connection_id {}".format(connection_id))
                conn = self.pool[backend_id][connection_id]
            else:
                # not found, so create and return
                # backend not found - create connection and asssign
                conn = backend_object.create_connection(
                    user_name,
                    workspace,
                    credentials,
                    mode
                )
                if settings.TESTING:
                    print("Creating new connection_id {}".format(connection_id))
                self.pool[backend_id][connection_id] = conn
        return conn


    def close_connection(self,
                         backend_object,
                         mig_req = None,
                         credentials = None,
                         mode="upload",
                         thread_number=None,
                         uid=""
        ):
        """Close the connection and remove it from the dictionary for the
        backend."""
        backend_id = backend_object.get_id()
        if mig_req is not None:
            connection_number = mig_req.pk
        else:
            connection_number = 0
        thread_id = ConnectionPool.__get_connection_id(
            connection_number,
            thread_number,
            uid,
            mode
        )
        if backend_id in self.pool and thread_id in self.pool[backend_id]:
            backend_object.close_connection(self.pool[backend_id][thread_id])
            self.pool[backend_id].pop(thread_id)
            if settings.TESTING:
                print("Closing connection {}".format(thread_id))


    def close_all_connections(self):
        # close the connections
        for backend_id in self.pool:
            backend_object = jdma_control.backends.Backend.get_backend_object(
                backend_id
            )
            for mig_id in self.pool[backend_id]:
                backend_object.close_connection(self.pool[backend_id][mig_id])
