# coding: utf-8
from __future__ import unicode_literals, division, print_function, absolute_import

from nose.tools import eq_, assert_raises
from marrow.wsgi.objects.request import LocalRequest
from marrow.wsgi.exceptions import HTTPFound
from sqlalchemy import Column, Integer, Unicode
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from web.core.application import Application


Base = declarative_base()
Session = scoped_session(sessionmaker())


class DummyException(Exception):
    pass


class DummyModel(Base):
    __tablename__ = 'dummy'
    id = Column(Integer, primary_key=True)
    name = Column(Unicode, nullable=False)


def commit_controller(context):
    Session.add(DummyModel(name='foo'))


def rollback_controller(context):
    Session.add(DummyModel(name='baz'))
    raise DummyException


def httpexception_controller(context):
    Session.add(DummyModel(name='bar'))
    raise HTTPFound(location=b'/')


class TestSQLAlchemyExtension(object):
    default_config = {
            'extensions': {
                    'sqlalchemy': {
                            'url': 'sqlite:///',
                            'session': Session,
                            'metadata': Base.metadata
                        }
                }
        }

    def teardown(self):
        Session.remove()
        Session.configure(bind=None)
        Base.metadata.bind = None

    def test_automatic_commit(self):
        app = Application(commit_controller, self.default_config)
        Base.metadata.create_all()
        request = LocalRequest()
        status, headers, body = app(request.environ)

        obj = Session.query(DummyModel).first()

        eq_(status, b'200 OK')
        eq_(obj.name, 'foo')
        assert obj.id > 0

    def test_rollback_on_exception(self):
        app = Application(rollback_controller, self.default_config)
        Base.metadata.create_all()
        request = LocalRequest()

        assert_raises(DummyException, app, request.environ)
        eq_(Session.query(DummyModel).first(), None)

    def test_commit_on_httpexception(self):
        app = Application(httpexception_controller, self.default_config)
        Base.metadata.create_all()
        request = LocalRequest()
        status, headers, body = app(request.environ)

        eq_(list(Session.new), [])  # make sure that either commit or rollback has happened
        obj = Session.query(DummyModel).first()

        eq_(status, b'302 Found')
        eq_(obj.name, 'bar')
        assert obj.id > 0
