#!/usr/bin/env python
# -*- coding: utf-8 -*-

from ldap3 import Server, Connection, ALL, SUBTREE, OFFLINE_AD_2012_R2
import re
import sys
sys.path.insert(0, '/var/www/flaskr') # required for from flaskr import
from myflaskrsecrets import ldapserver, ldapuser, ldappwd, ldapdomain

class ADAuthenticated():
    def __init__(self, username, password=None):
        self.ldap3_server_get_info = None
        # Other options are ALL (get schema from server) and OFFLINE_AD_2012_R2 (get schema
        # from ldap3 json). Both add substantial connection latency to every request to a
        # route with @login_required (not just the /login route).
        
        self.ldap_server = Server(ldapserver,
                                  get_info=self.ldap3_server_get_info)
        self.username = username
        self.ldapdomain = ldapdomain
        llen = len(self.ldapdomain)
        if username and not re.search('[@\\\]',username):
            self.username = "{}{}".format(self.username, self.ldapdomain)
            print("username tweaked to {}".format(self.username))
        self.password = password
        self.base_dn = 'DC=risd,DC=k12,DC=nm,DC=us' # push this back to myflasksecrets someday
        # CN=ldap connector,OU=Non-MailboxUsers,DC=risd,DC=k12,DC=nm,DC=us
        # self.admin_dn = "OU=ADMIN,OU=RISD,{}".format(self.base_dn)
        # self.studentroot = "OU=STUDENTS,OU=RISD,{}".format(self.base_dn)
        # CN=Bryan Yates,OU=ADMIN,OU=RISD,DC=risd,DC=k12,DC=nm,DC=us
        # self.staff_dn = "OU=RISD,{}".format(self.base_dn)

    def is_authentic(self):
        '''Returns True if user authenticates against AD. False otherwise'''
        #not sure why you can bind with empty string for password even with a fake username. seems odd and I don't remember this being the case initially,... but in any case, if password is empty string, return false
        if self.password == '':
            return False
        try:
            with Connection(self.ldap_server, user=self.username, password=self.password) as conn:
                # print("Description of result: {}".format(conn.result["description"])) # "success" if bind is ok
                return True
        except:
            print('Unable to connect to LDAP server')
            return False

        # for i in self.OU_groups:
        #     con_string = self.OU_groups[i]
        #     try:
        #         self.connect = Connection(self.ldap_server, con_string, self.password, auto_bind=True, raise_exceptions=True)
        #         return True
        #     except:
        #         pass
        # return False

    # def find_OU(self):
    #     '''Returns group top level OU group of user or None if not authed'''
    #     for i in self.OU_groups:
    #         con_string = self.OU_groups[i]
    #         try:
    #             self.connect = Connection(self.ldap_server, con_string, self.password, auto_bind=True)
    #             return i 
    #         except:
    #             pass
    #     return None

    def get_ADInfo(self):
        '''Uses service account to retrieve AD information. Since OU is not necessarily knowable (if web user arrives pre-authenticated and does not enter password), all OU connection DNs (see __init__ above) are tried. If information is not retrieved with one of the DN's or connection/binding otherwise fail, returns None.'''
        service_account_name = ldapuser
        service_account_password = ldappwd
        service_connection_string = "CN=ldap connector,OU=Non-MailboxUsers,DC=risd,DC=k12,DC=nm,DC=us"
        connect = Connection(self.ldap_server, service_connection_string, service_account_password, auto_bind=True)
        requested_user_attributes = ['displayName', 'sn', 'givenName', 'mail']

        user_info = {}
        for OU_friendly_name in self.OU_groups.keys():
            try:
                user_found = connect.search(search_base = self.OU_groups[OU_friendly_name], search_scope = SUBTREE, search_filter = ('(sAMAccountName=%s)' % self.username), attributes = requested_user_attributes)
                if user_found is not True:
                    continue
                    
                user_info['OU'] = OU_friendly_name
                ldap_user_search_result = connect.response[0]
                for requested_user_attribute in requested_user_attributes:
                    user_info[requested_user_attribute] = ldap_user_search_result['attributes'][requested_user_attribute]
                    if self.ldap3_server_get_info == None:
                        # Schema was loaded neither from server nor from an offline model
                        # This causes single items to be returned as the lone elements of
                        # lists when they would otherwise be returned as values.
                        try:
                            user_info[requested_user_attribute] = user_info[requested_user_attribute][0]
                        except IndexError:
                            # When the attribute does not have a value, AD returns an empty list.
                            user_info[requested_user_attribute] = None
                user_distinguished_name = ldap_user_search_result["dn"]
                connect.search(search_base = "OU=Groups,DC=NMMI,DC=local",
                               search_scope = SUBTREE,
                               #member:1.2.840.113556.1.4.1941: means recursive group membership
                               search_filter = f"(member:1.2.840.113556.1.4.1941:={user_distinguished_name})",
                               attributes = None)
                user_info['memberOf'] = [re.search('^CN=([^,]+)',entry["dn"]).group(1) for entry in connect.response]
                break
            except Exception as e:
                print(e, "error")
                pass

        if 'OU' in user_info:
            return user_info
        else:
            return None
###########################
#the below is a manual test.
if __name__ == '__main__':
    import getpass
    username = input('username? ')
    password = getpass.getpass('Password? ')
    print("username={}".format(username))
    ad_conn = ADAuthenticated(username=username, password=password)
    # print("ad_conn.is_authentic()={}".format(ad_conn.is_authentic()))

    def testPassAuth():
        #test is authenticated
        print ('***************\n')
        print ('Authentication Result:')
        if ad_conn.is_authentic():
            print ('Authenticate with username,password:')
            print ('Passed\n\n')
        else:
            print ('Fail\n\n')
        #test top level OU membership
        # group = ad_conn.find_OU()
        # if group is not None:
        #     print ('Group Membership: \n%s' % group)
        # else:
        #     print ('Not authenticated or member of no groups')
    
    # def testSearch():
    #     groups = ad_conn.get_ADInfo()
    #     if groups is None:
    #         print ('error, AD groups not found')
    #     else:
    #         print (groups)
    #         print ()
    #         print (groups['memberOf'])
    testPassAuth()
    # testSearch()
    # testPeopleCode()
