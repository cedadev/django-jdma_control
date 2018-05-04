"""A container class to create and access connections to the various
backends."""
import jdma_control.backends

class ConnectionPool:
    """A container class to create and access connections to the various
    backends."""
    def __init__(self):
        self.pool = {}

    def __get_connection_id(connection_id, thread_number):
        if thread_number == None:
            new_connection_id = str(connection_id)
        else:
            new_connection_id = "{}_{}".format(
                str(connection_id),
                str(thread_number)
            )
        return new_connection_id


    def find_or_create_connection(self,
                                  backend_object,
                                  mig_req,
                                  credentials,
                                  mode="upload",
                                  thread_number=None
        ):
        """The connection pool is a dictionary of dictionaries:
        {backend_id : {mig_id, connection_object }}"""
        backend_id = backend_object.get_id()
        connection_id = ConnectionPool.__get_connection_id(
            mig_req.pk,
            thread_number
        )
        if backend_id not in self.pool:
            # backend not found - create connection and asssign
            conn = backend_object.create_connection(
                mig_req.migration.user.name,
                mig_req.migration.workspace.workspace,
                credentials,
                mode
            )
            conn_dict = {connection_id : conn}
            self.pool[backend_id] = conn_dict
        else:
            # search for mig_req.pk in the backend
            if connection_id in self.pool[backend_id]:
                # found
                conn = self.pool[backend_id][connection_id]
            else:
                # not found, so create and return
                # backend not found - create connection and asssign
                conn = backend_object.create_connection(
                    mig_req.migration.user.name,
                    mig_req.migration.workspace.workspace,
                    credentials,
                    mode
                )
                self.pool[backend_id][connection_id] = conn
        return conn


    def close_connection(self, backend_object, mig_req, thread_number=None):
        """Close the connection and remove it from the dictionary for the
        backend."""
        backend_id = backend_object.get_id()
        thread_id = ConnectionPool.__get_connection_id(
            mig_req.pk,
            thread_number
        )
        if backend_id in self.pool and thread_id in self.pool[backend_id]:
            backend_object.close_connection(self.pool[backend_id][thread_id])
            self.pool[backend_id].pop(thread_id)


    def close_all_connections(self):
        # close the connections
        for backend_id in self.pool:
            backend_object = jdma_control.backends.Backend.get_backend_object(
                backend_id
            )
            for mig_id in self.pool[backend_id]:
                backend_object.close_connection(self.pool[backend_id][mig_id])