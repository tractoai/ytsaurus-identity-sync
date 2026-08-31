package ldap

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/tractoai/ytsaurus-identity-sync/internal/config"

	"go.ytsaurus.tech/library/go/ptr"
)

const (
	openLDAPImage         = "osixia/openldap:2.6.10-alpha"
	openLDAPPort          = "3890/tcp"
	openLDAPAdminDN       = "cn=admin,dc=example,dc=org"
	openLDAPAdminPassword = "adminpassword"
	// openLDAPAdminPasswordHash is an SSHA hash of "adminpassword". Generate an equivalent hash
	// (SSHA uses a random salt, so the value will differ) with:
	// docker run --rm osixia/openldap:2.6.10-alpha run -- slappasswd -s adminpassword
	openLDAPAdminPasswordHash = "{SSHA}uP0zxTVhTcxTcOlmwuuEeGtlBZJD2vZ5"
)

type OpenLdapLocal struct {
	container testcontainers.Container
}

func NewOpenLdapLocal() *OpenLdapLocal {
	return &OpenLdapLocal{}
}

func (y *OpenLdapLocal) Start() error {
	ctx := context.Background()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: openLDAPImage,
			Env: map[string]string{
				"OPENLDAP_BOOTSTRAP_ORGANIZATION":              "Example",
				"OPENLDAP_BOOTSTRAP_SUFFIX":                    "dc=example,dc=org",
				"OPENLDAP_BOOTSTRAP_DATA_ROOT_PASSWORD_HASHED": openLDAPAdminPasswordHash,
			},
			ExposedPorts: []string{openLDAPPort},
			WaitingFor: wait.ForAll(
				wait.ForLog("slapd starting"),
				wait.ForExec([]string{
					"ldapwhoami",
					"-x",
					"-H", "ldap://localhost:3890",
					"-D", openLDAPAdminDN,
					"-w", openLDAPAdminPassword,
				}),
			).
				WithStartupTimeout(2 * time.Minute),
		},
		Started: true,
	})
	if err != nil {
		return err
	}
	y.container = container
	return nil
}

func (y *OpenLdapLocal) GetConfig() (*config.LdapConfig, error) {
	ctx := context.Background()
	port, err := y.container.MappedPort(ctx, openLDAPPort)
	if err != nil {
		return nil, err
	}
	host, err := y.container.Host(ctx)
	if err != nil {
		return nil, err
	}
	connectionString := fmt.Sprintf("ldap://%s", net.JoinHostPort(host, port.Port()))
	return &config.LdapConfig{
		Address:            connectionString,
		BaseDN:             "dc=example,dc=org",
		BindDN:             openLDAPAdminDN,
		BindPasswordEnvVar: "LDAP_PASSWORD",
		Users: config.LdapUsersConfig{
			Filter:                 "(&(objectClass=posixAccount)(ou=People))",
			UsernameAttributeType:  "cn",
			UIDAttributeType:       "uid",
			FirstNameAttributeType: ptr.String("givenName"),
		},
		Groups: config.LdapGroupsConfig{
			Filter:                 "(objectClass=posixGroup)",
			GroupnameAttributeType: "cn",
			MemberUIDAttributeType: "memberUid",
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
