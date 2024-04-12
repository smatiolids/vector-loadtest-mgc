# Astra Connection (Based on cqlsession, from cassio library)
import os
from cassandra.cluster import (
    Cluster,
)
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import dict_factory

def getCQLSession(mode='astra_db'):
    print('Initializing CQL Session')
    if mode == 'astra_db':
        cluster = Cluster(
            cloud={
                "secure_connect_bundle": os.environ["ASTRA_DB_SECURE_BUNDLE_PATH"],
            },
            auth_provider=PlainTextAuthProvider(
                os.environ["ASTRA_DB_CLIENT_ID"],
                os.environ["ASTRA_DB_CLIENT_SECRET"],
            ),
        )
        astraSession = cluster.connect()
        astraSession.row_factory = dict_factory
        print('Connected')
        return astraSession
    elif mode == 'dse':
        cluster = Cluster(os.environ["DSE_NODES"].split(','), auth_provider=PlainTextAuthProvider(
        username=os.environ["DSE_USER"], password=os.environ['DSE_PASS']))
        dseSession = cluster.connect()
        return dseSession
    elif mode == 'local':
        cluster = Cluster()
        localSession = cluster.connect()
        return localSession
    else:
        raise ValueError('Unknown CQL Session mode')

def getCQLKeyspace():
    return os.environ["ASTRA_DB_KEYSPACE"]
