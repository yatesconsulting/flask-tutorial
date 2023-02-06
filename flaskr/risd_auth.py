#!/usr/bin/env python
# -*- coding: utf-8 -*-

from ldap3 import Server, Connection, ALL, SUBTREE, OFFLINE_AD_2012_R2
import re
import sys
sys.path.insert(0, '/var/www/flaskr') # required for from flaskr import
from myflaskrsecrets import ldapserver, ldapuser, ldappwd, ldapdomain, ldapdcroot

class ADAuthenticated():
    def __init__(self, username, password=None):
        self.ldap3_server_get_info = None
        # Other options are ALL (get schema from server) and OFFLINE_AD_2012_R2 (get schema
        # from ldap3 json). Both add substantial connection latency to every request to a
        # route with @login_required (not just the /login route).

        self.ldap_server = Server(ldapserver, get_info=self.ldap3_server_get_info)
        self.username = username
        self.password = password
        self.ldapdomain = ldapdomain
        self.groups = []
        self.firstname = "null"
        self.lastname = "null"

        if username and not re.search('[@\\\]',username):
            self.username = "{}{}".format(self.username, self.ldapdomain)
        #     # print("username tweaked to {}".format(self.username))
        self.authenticated = self.is_authentic()
        # self.refreshgroups()

    def __str__(self):
        return 'username {}, {} {} with auth {}'.format( "{}{}".format(self.username, self.ldapdomain), self.firstname, self.lastname, self.authenticated)


    def is_authentic(self):
        ''' Returns False if user authenticates and is in student group, or
            Returns True if user authenticates against AD, or
            False otherwise'''

        self.groups = []

        if self.password == '': # binding with no pw not allowed here
            return False
        try:
            # print("36 trying to connect to {} with username {}".format(self.ldap_server, self.username))

            with Connection(self.ldap_server, user=self.username, password=self.password) as conn:
                # print("Description of result: {}".format(conn.result["description"])) # "success" if bind is ok
                # print("maybe {}".format(conn.entries)) # .memberOf.values))

                # print("conN:{} Description of result: {}".format(str(conn), conn.result["description"])) # "success" if bind is ok
                conn.search(
                    search_base=ldapdcroot,
                    search_filter = "(&(userPrincipalName={}))".format(self.username),
                    # search_scope='SUBTREE',
                    attributes=['memberOf','givenName','sn'])
                for entry in conn.entries:
                    for gp in entry.memberOf.values:
                        # print ("entry: {}".format(gp))
                        pgp = re.search('CN=([^,]*),.*$',gp)
                        if pgp:
                            self.groups.append(pgp.group(1).lower())
                        else:
                            self.groups.append(gp)

                    self.firstname = entry.givenName.value
                    self.lastname = entry.sn.value

                if self.in_group("students"):
                    return False
                return True # bads will fail out via except
        except:
            print('65 Unable to connect to LDAP server')
            return False

    def in_group(self, group=""):
        # regexp = re.compile('^cn={},'.format(group.lower()))

        if self.password == '':
            return False
        # if not self.groups:
        #     # print("refreshing groups")
        #     self.refreshgroups()

        if group == "":
            return self.groups
        else:
            return group.lower() in self.groups
            # for g in self.groups:
            #     # user_info['memberOf'] = [re.search('^CN=([^,]+)',entry["dn"]).group(1) for entry in connect.response]
            #     if regexp.search(g.lower()): # bool(re.search('^cn={},'.format(group.lower()), g.lower)) fails for some reason
            #         return True
            # return False

###########################
#the below is a manual test.
if __name__ == '__main__':
    import getpass
    username = input('username? ')
    password = getpass.getpass('Password? ')
    # print("username={}".format(username))
    ad_conn = ADAuthenticated(username=username, password=password)
    # print ("ad_conn.is_authentic()={}".format(ad_conn.is_authentic()))
    print ("ad_conn.authenticated = {}".format(ad_conn.authenticated))
    print ("in IT? {}".format(ad_conn.in_group('ITDEpartment')))
    print ("in students? {}".format(ad_conn.in_group('students')))
    print ("all groups = {}".format(ad_conn.groups))
    print ("ad_conn = {}".format(ad_conn))
    print ("strad_conn = {}".format(str(ad_conn)))
    print ("firstname = {}".format(ad_conn.firstname))
    print ("lastname = {}".format(ad_conn.lastname))

