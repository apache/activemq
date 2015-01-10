package org.apache.activemq.transport.tcp;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.ModificationItem;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.LdapContext;

import org.apache.activemq.transport.TransportBrokerTestSupport;
import org.apache.commons.io.FileUtils;
import org.apache.directory.api.ldap.model.constants.SchemaConstants;
import org.apache.directory.server.annotations.CreateLdapServer;
import org.apache.directory.server.constants.ServerDNConstants;
import org.apache.directory.server.core.api.CoreSession;
import org.apache.directory.server.core.api.DirectoryService;
import org.apache.directory.server.core.factory.DSAnnotationProcessor;
import org.apache.directory.server.core.factory.DefaultDirectoryServiceFactory;
import org.apache.directory.server.core.factory.DirectoryServiceFactory;
import org.apache.directory.server.core.factory.PartitionFactory;
import org.apache.directory.server.core.jndi.CoreContextFactory;
import org.apache.directory.server.factory.ServerAnnotationProcessor;
import org.apache.directory.server.i18n.I18n;
import org.apache.directory.server.kerberos.kdc.KdcServer;
import org.apache.directory.server.kerberos.shared.crypto.encryption.KerberosKeyFactory;
import org.apache.directory.server.kerberos.shared.keytab.Keytab;
import org.apache.directory.server.kerberos.shared.keytab.KeytabEntry;
import org.apache.directory.server.ldap.LdapServer;
import org.apache.directory.shared.kerberos.KerberosTime;
import org.apache.directory.shared.kerberos.codec.types.EncryptionType;
import org.apache.directory.shared.kerberos.components.EncryptionKey;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Krb5BrokerTestSupport extends TransportBrokerTestSupport {
    private static final String KERBEROS_PRINCIPAL_USER1 = "user1@EXAMPLE.COM";
    private static final String KERBEROS_PRINCIPAL_USER2 = "user2@EXAMPLE.COM";
    private static final String KERBEROS_PRINCIPAL_PASSWORD = "Piotr Klimczak";

    private static final Logger LOG = LoggerFactory.getLogger(Krb5BrokerTestSupport.class);

    /** The used DirectoryService instance */
    public static DirectoryService service;

    /** The used LdapServer instance */
    public static LdapServer ldapServer;

    /** The used KdcServer instance */
    public static KdcServer kdcServer;

    private DirContext ctx;

    /** the context root for the schema */
    protected LdapContext schemaRoot;

    /** the context root for the system partition */
    protected LdapContext sysRoot;

    /** the context root for the rootDSE */
    protected CoreSession rootDse;

    private static boolean initialized = false;
    private static String hostname;

    public Krb5BrokerTestSupport() {
        super();

        if (!initialized) {
            init();
            initialized = true;
        }
    }

    private void init() {
        try {
            hostname = InetAddress.getLocalHost().getHostName();
            configureSystemProperties();
            createServer();
            DirContext server = setupServer();
            setPrincipals(server);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected void configureSystemProperties() throws IOException {
        String basedir = System.getProperty("basedir");
        if (basedir == null) {
            basedir = new File(".").getCanonicalPath();
        }
        File krb5conf = new File(basedir + "/src/test/resources/kerberos/krb5.conf");
        System.setProperty("java.security.krb5.conf", krb5conf.getPath());
        System.setProperty("sun.security.krb5.debug", "false");

        File loginModuleConf = new File(basedir + "/target/LoginModule.conf");
        FileUtils.write(loginModuleConf, getLoginModuleFileContent(hostname));
        System.setProperty("java.security.krb5.conf", krb5conf.getPath());
        System.setProperty("java.security.auth.login.config", loginModuleConf.getPath());
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "true");
    }

    protected void setPrincipals(DirContext users) throws UnknownHostException, NamingException, IOException {
        Attributes attrs;
        String hostPrincipal = "host/" + hostname + "@EXAMPLE.COM";

        attrs = getPrincipalAttributes("Service", "KDC Service", "krbtgt", KERBEROS_PRINCIPAL_PASSWORD, "krbtgt/EXAMPLE.COM@EXAMPLE.COM");
        users.createSubcontext("uid=krbtgt", attrs);

        attrs = getPrincipalAttributes("Service", "LDAP Service", "ldap", KERBEROS_PRINCIPAL_PASSWORD, "ldap/localhost@EXAMPLE.COM");
        users.createSubcontext("uid=ldap", attrs);

        attrs = getPrincipalAttributes("Service", "AMQ Service", "host", KERBEROS_PRINCIPAL_PASSWORD, hostPrincipal);
        users.createSubcontext("uid=host", attrs);

        attrs = getPrincipalAttributes("Service", "AMQ Service", "user1", KERBEROS_PRINCIPAL_PASSWORD, KERBEROS_PRINCIPAL_USER1);
        users.createSubcontext("uid=user1", attrs);

        attrs = getPrincipalAttributes("Service", "AMQ Service", "user2", KERBEROS_PRINCIPAL_PASSWORD, KERBEROS_PRINCIPAL_USER2);
        users.createSubcontext("uid=user2", attrs);

        createKeytab(hostPrincipal, KERBEROS_PRINCIPAL_PASSWORD, new File("target/Broker.keytab"));
        createKeytab(KERBEROS_PRINCIPAL_USER1, KERBEROS_PRINCIPAL_PASSWORD, new File("target/User1.keytab"));
        createKeytab(KERBEROS_PRINCIPAL_USER2, KERBEROS_PRINCIPAL_PASSWORD, new File("target/User2.keytab"));
    }

    /**
     * Creates a keytab file for given principal.
     * 
     * @param principalName
     * @param passPhrase
     * @param keytabFile
     * @throws IOException
     */
    public static void createKeytab(final String principalName, final String passPhrase, final File keytabFile) throws IOException {
        final KerberosTime timeStamp = new KerberosTime();
        final int principalType = 1; // KRB5_NT_PRINCIPAL

        final Keytab keytab = Keytab.getInstance();
        final List<KeytabEntry> entries = new ArrayList<KeytabEntry>();
        for (Map.Entry<EncryptionType, EncryptionKey> keyEntry : KerberosKeyFactory.getKerberosKeys(principalName, passPhrase).entrySet()) {
            final EncryptionKey key = keyEntry.getValue();
            final byte keyVersion = (byte) key.getKeyVersion();
            entries.add(new KeytabEntry(principalName, principalType, timeStamp, keyVersion, key));
        }
        keytab.setEntries(entries);
        keytab.write(keytabFile);
    }

    protected Attributes getPrincipalAttributes(String sn, String cn, String uid, String userPassword, String principal) {
        Attributes attrs = new BasicAttributes(true);
        Attribute ocls = new BasicAttribute("objectClass");
        ocls.add("top");
        ocls.add("person"); // sn $ cn
        ocls.add("inetOrgPerson"); // uid
        ocls.add("krb5principal");
        ocls.add("krb5kdcentry");
        attrs.put(ocls);
        attrs.put("cn", cn);
        attrs.put("sn", sn);
        attrs.put("uid", uid);
        attrs.put("userPassword", userPassword);
        attrs.put("krb5PrincipalName", principal);
        attrs.put("krb5KeyVersionNumber", "0");

        return attrs;
    }

    private DirContext setupServer() throws Exception, NamingException, UnknownHostException, IOException {
        Attributes attrs;

        setContexts("uid=admin,ou=system", "secret");

        // check if krb5kdc is disabled
        Attributes krb5kdcAttrs = schemaRoot.getAttributes("cn=Krb5kdc");
        boolean isKrb5KdcDisabled = false;

        if (krb5kdcAttrs.get("m-disabled") != null) {
            isKrb5KdcDisabled = ((String) krb5kdcAttrs.get("m-disabled").get()).equalsIgnoreCase("TRUE");
        }

        // if krb5kdc is disabled then enable it
        if (isKrb5KdcDisabled) {
            Attribute disabled = new BasicAttribute("m-disabled");
            ModificationItem[] mods = new ModificationItem[] { new ModificationItem(DirContext.REMOVE_ATTRIBUTE, disabled) };
            schemaRoot.modifyAttributes("cn=Krb5kdc", mods);
        }

        // Get a context, create the ou=users subcontext, then create the 3
        // principals.
        Hashtable<String, Object> env = new Hashtable<String, Object>();
        env.put(DirectoryService.JNDI_KEY, getService());
        env.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.directory.server.core.jndi.CoreContextFactory");
        env.put(Context.PROVIDER_URL, "dc=example,dc=com");
        env.put(Context.SECURITY_PRINCIPAL, "uid=admin,ou=system");
        env.put(Context.SECURITY_CREDENTIALS, "secret");
        env.put(Context.SECURITY_AUTHENTICATION, "simple");

        ctx = new InitialDirContext(env);

        attrs = getOrgUnitAttributes("users");
        DirContext users = ctx.createSubcontext("ou=users", attrs);
        return users;
    }

    private Description getDescription() {
        return Description.createSuiteDescription(this.getClass().getName(), this.getClass().getAnnotations());
    }

    private void createServer() {
        try {
            CreateLdapServer classLdapServerBuilder = getDescription().getAnnotation(CreateLdapServer.class);
            service = DSAnnotationProcessor.getDirectoryService(getDescription());

            DirectoryService directoryService = null;

            if (service != null) {
                // We have a class DS defined, update it
                directoryService = service;

                DSAnnotationProcessor.applyLdifs(getDescription(), service);
            } else {
                // No : define a default class DS then
                DirectoryServiceFactory dsf = DefaultDirectoryServiceFactory.class.newInstance();

                directoryService = dsf.getDirectoryService();
                // enable CL explicitly cause we are not using
                // DSAnnotationProcessor
                directoryService.getChangeLog().setEnabled(true);

                dsf.init("default" + UUID.randomUUID().toString());

                // Stores the defaultDS in the classDS
                service = directoryService;

                // Load the schemas
                DSAnnotationProcessor.loadSchemas(getDescription(), directoryService);

                // Apply the class LDIFs
                DSAnnotationProcessor.applyLdifs(getDescription(), directoryService);
            }

            // check if it has a LdapServerBuilder
            // then use the DS created above
            if (classLdapServerBuilder != null) {
                ldapServer = ServerAnnotationProcessor.createLdapServer(getDescription(), directoryService);
            }

            if (kdcServer == null) {
                int minPort = getMinPort();

                kdcServer = ServerAnnotationProcessor.getKdcServer(getDescription(), directoryService, minPort + 1);
            }

            // print out information which partition factory we use
            DirectoryServiceFactory dsFactory = DefaultDirectoryServiceFactory.class.newInstance();
            PartitionFactory partitionFactory = dsFactory.getPartitionFactory();
            LOG.debug("Using partition factory {}", partitionFactory.getClass().getSimpleName());
        } catch (Exception e) {
            LOG.error(I18n.err(I18n.ERR_181, this.getClass().getName()));
            LOG.error(e.getLocalizedMessage());
        }
    }

    /**
     * Convenience method for creating an organizational unit.
     * 
     * @param ou
     *            the ou of the organizationalUnit
     * @return the attributes of the organizationalUnit
     */
    protected Attributes getOrgUnitAttributes(String ou) {
        Attributes attrs = new BasicAttributes(true);
        Attribute ocls = new BasicAttribute("objectClass");
        ocls.add("top");
        ocls.add("organizationalUnit");
        attrs.put(ocls);
        attrs.put("ou", ou);

        return attrs;
    }

    protected void setContexts(String user, String passwd) throws Exception {
        Hashtable<String, Object> env = new Hashtable<String, Object>();
        env.put(DirectoryService.JNDI_KEY, getService());
        env.put(Context.SECURITY_PRINCIPAL, user);
        env.put(Context.SECURITY_CREDENTIALS, passwd);
        env.put(Context.SECURITY_AUTHENTICATION, "simple");
        env.put(Context.INITIAL_CONTEXT_FACTORY, CoreContextFactory.class.getName());

        Hashtable<String, Object> envFinal = new Hashtable<String, Object>(env);
        envFinal.put(Context.PROVIDER_URL, ServerDNConstants.SYSTEM_DN);
        sysRoot = new InitialLdapContext(envFinal, null);

        envFinal.put(Context.PROVIDER_URL, "");
        rootDse = getService().getAdminSession();

        envFinal.put(Context.PROVIDER_URL, SchemaConstants.OU_SCHEMA);
        schemaRoot = new InitialLdapContext(envFinal, null);
    }

    /**
     * Get the lower port out of all the transports
     */
    private int getMinPort() {
        int minPort = 0;

        return minPort;
    }

    public static DirectoryService getService() {
        return service;
    }

    public static void setService(DirectoryService service) {
        Krb5BrokerTestSupport.service = service;
    }

    public static LdapServer getLdapServer() {
        return ldapServer;
    }

    public static void setLdapServer(LdapServer ldapServer) {
        Krb5BrokerTestSupport.ldapServer = ldapServer;
    }

    public static KdcServer getKdcServer() {
        return kdcServer;
    }

    public static void setKdcServer(KdcServer kdcServer) {
        Krb5BrokerTestSupport.kdcServer = kdcServer;
    }

    protected String getLoginModuleFileContent(String hostname) {
        return 
        "User1 {\n"+
        "    com.sun.security.auth.module.Krb5LoginModule required\n"+
        "    useKeyTab=true\n"+
        "    useTicketCache=false\n"+
        "    keyTab=\"file:./target/User1.keytab\"\n"+
        "    principal=\"user1@EXAMPLE.COM\";\n"+
        "};\n"+
        "\n"+
        "User2 {\n"+
        "    com.sun.security.auth.module.Krb5LoginModule required\n"+
        "    useKeyTab=true\n"+
        "    useTicketCache=false\n"+
        "    keyTab=\"file:./target/User2.keytab\"\n"+
        "    principal=\"user2@EXAMPLE.COM\";\n"+
        "};\n"+
        "\n"+
        "Broker {\n"+
        "    com.sun.security.auth.module.Krb5LoginModule required\n"+
        "    useKeyTab=true\n"+
        "    storeKey=true\n"+
        "    useTicketCache=false\n"+
        "    keyTab=\"file:./target/Broker.keytab\"\n"+
        "    principal=\"host/" + hostname + "@EXAMPLE.COM\";\n"+
        "};\n"+
        "\n"+
        "LDAPServer {\n"+
        "    com.sun.security.auth.module.Krb5LoginModule required\n"+
        "    useKeyTab=true\n"+
        "    storeKey=true\n"+
        "    useTicketCache=false\n"+
        "    keyTab=\"file:./target/LDAPServer.keytab\"\n"+
        "    principal=\"ldap/localhost@EXAMPLE.COM\";\n"+
        "};";
    }
}
