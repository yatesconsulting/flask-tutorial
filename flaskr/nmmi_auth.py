#!/usr/bin/env python
# -*- coding: utf-8 -*-
​
from models.dbconnectors import NMMIDBConnector
from ldap3 import Server, Connection, ALL, SUBTREE, OFFLINE_AD_2012_R2
import re
​
class nmmiAuthGroup():
    def __init__(self, username, password=None):
        self.ldap3_server_get_info = None
        # Other options are ALL (get schema from server) and OFFLINE_AD_2012_R2 (get schema
        # from ldap3 json). Both add substantial connection latency to every request to a
        # route with @login_required (not just the /login route).
        
        self.ldap_server = Server('REDACTED - was the hostname for our domain controller',
                                  get_info=self.ldap3_server_get_info)
        self.username = username
        self.password = password
        self.admin_dn = "CN=%s,OU=%s,OU=%s,DC=%s,DC=%s" % (username, 'Users', 'Admin', 'NMMI', 'local')
        self.staff_dn = "CN=%s,OU=%s,DC=%s,DC=%s" % (username, 'Staff', 'NMMI', 'local')
        self.fac_dn = "CN=%s,OU=%s,DC=%s,DC=%s" % (username, 'Faculty', 'NMMI', 'local')
        self.cadet_dn = "CN=%s,OU=%s,OU=%s,DC=%s,DC=%s" % (username, 'Users', 'Cadets', 'NMMI', 'local')
        self.selfservice_dn = "CN=%s,OU=%s,DC=%s,DC=%s" % (username, 'Self Service', 'NMMI', 'local')
        self.serviceaccounts_dn = "CN=%s,OU=%s,DC=%s,DC=%s" % (username, 'Service Accounts', 'NMMI', 'local')
        self.endofsemester_dn = "CN=%s,OU=%s,DC=%s,DC=%s" % (username, 'EndOfSemester', 'NMMI', 'local')
        self.base_dn = 'dc=NMMI,dc=local'
        if re.match('^[0-9]{9}$', username):
            # cadets and parents with powercampus ID-based usernames
            self.OU_groups = {
                'Cadet': self.cadet_dn,
                'Self Service': self.selfservice_dn,
                'EndOfSemester': self.endofsemester_dn
            }
        else:
            # users with non-powercampus ID-based usernames
            self.OU_groups = {
                'Admin': self.admin_dn,
                'Staff': self.staff_dn,
                'Faculty': self.fac_dn,
                'Service Accounts': self.serviceaccounts_dn,
                'Self Service': self.selfservice_dn # some users in the Parents sub-OU have non-numeric usernames
            }
​
    def is_authentic(self):
        '''Returns True if user authenticates against NMMI AD. False otherwise'''
        #not sure why you can bind with empty string for password even with a fake username. seems odd and I don't remember this being the case initially,... but in any case, if password is empty string, return false
        if self.password == '':
            return False
        for i in self.OU_groups:
            con_string = self.OU_groups[i]
            try:
                self.connect = Connection(self.ldap_server, con_string, self.password, auto_bind=True, raise_exceptions=True)
                return True
            except:
                pass
        return False
​
    def find_OU(self):
        '''Returns group top level OU group of user or None if not authed'''
        for i in self.OU_groups:
            con_string = self.OU_groups[i]
            try:
                self.connect = Connection(self.ldap_server, con_string, self.password, auto_bind=True)
                return i 
            except:
                pass
        return None
​
    def get_ADInfo(self):
        '''Uses service account to retrieve AD information. Since OU is not necessarily knowable (if web user arrives pre-authenticated and does not enter password), all OU connection DNs (see __init__ above) are tried. If information is not retrieved with one of the DN's or connection/binding otherwise fail, returns None.'''
        service_account_name = 'REDACTED - was the username used to connect to the domain controller for LDAP lookups'
        service_account_password = 'REDACTED - was the password used by the above account'
        service_connection_string = "CN=%s,OU=%s,DC=%s,DC=%s" % (service_account_name, 'Service Accounts', 'NMMI', 'local')
        connect = Connection(self.ldap_server, service_connection_string, service_account_password, auto_bind=True)
        requested_user_attributes = ['displayName', 'sn', 'givenName', 'mail']
​
        user_info = {}
        for OU_friendly_name in self.OU_groups.keys():
            try:
                user_found = connect.search(search_base = self.OU_groups[OU_friendly_name], search_scope = SUBTREE, search_filter = ('(sAMAccountName=%s)' % self.username), attributes = requested_user_attributes)
                if user_found is not True:
                    continue
​
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
​
        if 'OU' in user_info:
            return user_info
        else:
            return None
​
    def get_PeopleCode(self, nmmi_login):
        '''Returns people code id from Login. Tries nmmi employees, if no success, returns None'''
        dbcon = NMMIDBConnector()
        con_inst = dbcon.connect('Institute')
        dbc = con_inst[0] 
        cur_inst = con_inst[1]
        #There was a problem with web recieved content and database "right trunctiona error". This problem did not seem to show up under Windows.
        #An explicit set to string and a strip (newline, space) seems to solve it
        #nmmi_login = str(nmmi_login).strip()
        nmmi_login = str(nmmi_login)
        #try people id if is employee
        cur_inst.execute('select PEOPLE_ID from Employees where Email_Address=?', (nmmi_login,))
        test_id = cur_inst.fetchone()
        cur_inst.close()
        dbc.close()
        if test_id is not None and len(test_id) > 0:
            return test_id.PEOPLE_ID
        return None
​
###########################
#the below is a manual test.
if __name__ == '__main__':
    import getpass
    username = input('username? ')
    password = getpass.getpass('Password? ')
    ad_conn = nmmiAuthGroup(username, password)
    def testPassAuth():
        #test is authenticated
        print ('***************\n')
        print ('Authentication Result:')
        if ad_conn.is_authentic():
            print ('Authenticate with username,password:')
            print ('Passed\n\n')
        else:
            print ('Fail\n\n')
        #test top level OU group membership
        group = ad_conn.find_OU()
        if group is not None:
            print ('Group Membership: \n%s' % group)
        else:
            print ('Not authenticated or member of no groups')
    
    def testSearch():
        groups = ad_conn.get_ADInfo()
        if groups is None:
            print ('error, AD groups not found')
        else:
            print (groups)
            print ()
            print (groups['memberOf'])
​
    def testPeopleCode():
        people_code_id = ad_conn.get_PeopleCode(username)
        print (people_code_id)
​
    testPassAuth()
    testSearch()
    testPeopleCode()
