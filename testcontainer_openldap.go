package main

import (
	"context"
	"errors"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/openldap"
	"go.ytsaurus.tech/library/go/ptr"
)

type OpenLdapLocal struct {
	container *openldap.OpenLDAPContainer
}

func NewOpenLdapLocal() *OpenLdapLocal {
	return &OpenLdapLocal{}
}

func (y *OpenLdapLocal) Start() error {
	ctx := context.Background()
	container, err := openldap.RunContainer(ctx, testcontainers.WithImage("bitnami/openldap:2.6.6"))
	if err != nil {
		return err
	}
	y.container = container
	return y.container.Start(ctx)
}

func (y *OpenLdapLocal) GetConfig() (*LdapConfig, error) {
	connectionString, err := y.container.ConnectionString(context.Background())
	if err != nil {
		return nil, err
	}
	return &LdapConfig{
		Address:            connectionString, //"ldap://localhost:1389",
		BaseDN:             "dc=example,dc=org",
		BindDN:             "cn=admin,dc=example,dc=org",
		BindPasswordEnvVar: "LDAP_PASSWORD",
		Users: LdapUsersConfig{
			Filter:                 "(&(objectClass=posixAccount)(ou=People))",
			UsernameAttributeType:  "cn",
			UidAttributeType:       "uid",
			FirstNameAttributeType: ptr.String("givenName"),
		},
		Groups: LdapGroupsConfig{
			Filter:                 "(objectClass=posixGroup)",
			GroupnameAttributeType: "cn",
			MemberUidAttributeType: "memberUid",
		},
	}, nil
}

func (y *OpenLdapLocal) Stop() error {
	ctx := context.Background()
	if y.container == nil {
		return errors.New("container not started")
	}
	err := y.container.Terminate(ctx)
	if err != nil {
		return err
	}
	y.container = nil
	return nil
}
