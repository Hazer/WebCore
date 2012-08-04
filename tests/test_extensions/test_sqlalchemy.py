# coding: utf-8
from __future__ import unicode_literals, division, print_function, absolute_import

from nose.tools import eq_
from marrow.wsgi.objects.request import LocalRequest
from sqlalchemy import Column, Integer, Unicode
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from web.core.application import Application


Base = declarative_base()
Session = scoped_session(sessionmaker())


class DummyModel(Base):
    __tablename__ = 'dummy'
    id = Column(Integer, primary_key=True)
    name = Column(Unicode, nullable=False)


def commit_controller(context):
    Session.add(DummyModel(name='foo'))


def rollback_controller(context):
    Session.add(DummyModel(name='foo'))
    raise Exception


class TestSQLAlchemyExtension(object):
    default_config = {
            'extensions': {
                    'sqlalchemy': {
                            'url': 'sqlite:///',
                            'session': Session
                        }
                }
        }

    def test_automatic_commit(self):
        app = Application(commit_controller, self.default_config)
        request = LocalRequest()
        status, headers, body = app(request.environ)

        eq_(status, b'200 OK')

        obj = self.session.query().first()
        eq_(obj.name, 'name', self.dummy_user)
        assert obj.id > 0

    def test_rollback_on_exception(self):
        app = Application(rollback_controller, self.default_config)
        request = LocalRequest()
        status, headers, body = app(request.environ)

        eq_(status, b'500 Internal Server Error')
        eq_(self.session.query().first(), None)
