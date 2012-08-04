from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import ScopedSession


class SQLAlchemyExtension(object):
    uses = ['transaction']
    provides = ['sqlalchemy']

    def __init__(self, context, url, session, metadata=None, test_connection=True, **kwargs):
        super(SQLAlchemyExtension, self).__init__()

        if not isinstance(session, ScopedSession):
            raise TypeError('The "session" option needs to be a reference to a ScopedSession')
        if metadata is not None and not isinstance(metadata, MetaData):
            raise TypeError('The "metadata" option needs to be a reference to a MetaData')

        self._engine = create_engine(url, **kwargs)
        self._session = session
        self._metadata = metadata
        self._test_connection = test_connection

    def start(self, context):
        # Test the validity of the URL by attempting to make a connection to the target database
        if self._test_connection:
            self._engine.connect().close()

        # Bind the engine to the session to enable execution of raw SQL
        self._session.configure(bind=self._engine)

        # Bind the engine to the given metadata to facilitate operations like metadata.create_all()
        if self._metadata is not None:
            self._metadata.bind = self._engine

    def after(self, context, exc=None):
        if self._session.is_active:
            if exc is not None:
                self._session.rollback()
            else:
                self._session.commit()

        # Return the context-local connection to the connection pool
        self._session.close()
