/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
 * OpenSSL socket implementation, in large parts ported from C++.
 */
module thrift.transport.ssl;

import core.exception : onOutOfMemoryError;
import core.stdc.errno : getErrno, EINTR;
import core.stdc.string : strerror;
import core.sync.mutex : Mutex;
import core.memory : GC;
import core.stdc.config;
import core.stdc.stdlib : free, malloc;
import std.ascii : toUpper;
import std.array : empty, front, popFront;
import std.conv : emplace, to;
import std.exception : enforce;
import std.socket : InternetAddress, Socket;
import std.string : toStringz;
import thrift.internal.c.openssl.asn1;
import thrift.internal.c.openssl.bio;
import thrift.internal.c.openssl.crypto;
import thrift.internal.c.openssl.err;
import thrift.internal.c.openssl.objects;
import thrift.internal.c.openssl.rand;
import thrift.internal.c.openssl.safestack;
import thrift.internal.c.openssl.ssl;
import thrift.internal.c.openssl.x509;
import thrift.internal.c.openssl.x509_vfy;
import thrift.internal.c.openssl.x509v3;
import thrift.transport.base;
import thrift.transport.socket;

/**
 * SSL encrypted socket implementation using OpenSSL.
 */
final class TSSLSocket : TSocket {
  override bool isOpen() @property {
    if (ssl_ is null || !super.isOpen()) return false;

    auto shutdown = SSL_get_shutdown(ssl_);
    bool shutdownReceived = (shutdown & SSL_RECEIVED_SHUTDOWN) != 0;
    bool shutdownSent = (shutdown & SSL_SENT_SHUTDOWN) != 0;
    return !(shutdownReceived && shutdownSent);
  }

  override bool peek() {
    if (!isOpen) return false;
    checkHandshake();

    byte bt;
    auto rc = SSL_peek(ssl_, &bt, bt.sizeof);
    if (rc < 0) throw new TSSLException(getSSLErrorMessage(getErrno()));

    if (rc == 0) {
      ERR_clear_error();
    }
    return (rc > 0);
  }

  override void open() {
    enforce(!serverSide_, "Cannot open a server-side SSL socket.");
    if (isOpen) return;
    super.open();
  }

  override void close() {
    if (!isOpen) return;

    if (ssl_ !is null) {
      auto rc = SSL_shutdown(ssl_);
      if (rc == 0) {
        rc = SSL_shutdown(ssl_);
      }
      if (rc < 0) throw new TSSLException(getSSLErrorMessage(getErrno()));
      SSL_free(ssl_);
      ssl_ = null;
      ERR_remove_state(0);
    }
    super.close();
  }

  override size_t read(ubyte[] buf) {
    checkHandshake();

    int bytes;
    foreach (_; 0 .. maxRecvRetries) {
      bytes = SSL_read(ssl_, buf.ptr, cast(int)buf.length);
      if (bytes >= 0) break;

      auto errnoCopy = getErrno();
      if (SSL_get_error(ssl_, bytes) == SSL_ERROR_SYSCALL) {
        if (ERR_get_error() == 0 && errnoCopy == EINTR) {
          // FIXME: Windows.
          continue;
        }
      }
      throw new TSSLException(getSSLErrorMessage(errnoCopy));
    }
    return bytes;
  }

  override void write(in ubyte[] buf) {
    checkHandshake();

    // Loop in case SSL_MODE_ENABLE_PARTIAL_WRITE is set in SSL_CTX.
    size_t written = 0;
    while (written < buf.length) {
      auto bytes = SSL_write(ssl_, buf.ptr + written,
        cast(int)(buf.length - written));
      if (bytes <= 0) {
        throw new TSSLException(getSSLErrorMessage(getErrno()));
      }
      written += bytes;
    }
  }

  override void flush() {
    checkHandshake();

    auto bio = SSL_get_wbio(ssl_);
    enforce(bio !is null, new TSSLException("SSL_get_wbio returned null"));

    auto rc = BIO_flush(bio);
    enforce(rc == 1, new TSSLException(getSSLErrorMessage(getErrno())));
  }

  /**
   * Whether to use client or server side SSL handshake protocol.
   */
  bool serverSide() @property const {
    return serverSide_;
  }

  /// Ditto
  void serverSide(bool value) @property {
    serverSide_ = value;
  }

  /**
   * The access manager to use.
   */
  void accessManager(TAccessManager value) @property {
    accessManager_ = value;
  }

private:
  /**
   * Constructor that takes an already created, connected (!) socket.
   *
   * Params:
   *   factory = The SSL socket factory to use. Storing a reference to it also
   *     has the effect that it doesn't get cleaned up while the socket is used.
   *   socket = Already created, connected socket object.
   */
  this(TSSLSocketFactory factory, Socket socket) {
    super(socket);
    factory_ = factory;
  }

  /**
   * Creates a new unconnected socket that will connect to the given host
   * on the given port.
   *
   * Params:
   *   factory = The SSL socket factory to use. Storing a reference to it also
   *     has the effect that it doesn't get cleaned up while the socket is used.
   *   host = Remote host
   *   port = Remote port
   */
  this(TSSLSocketFactory factory, string host, ushort port) {
    super(host, port);
    factory_ = factory;
  }

  void checkHandshake() {
    enforce(super.isOpen(), new TTransportException(
      TTransportException.Type.NOT_OPEN));

    if (ssl_ !is null) return;
    ssl_ = SSL_new(factory_.context_.ctx);
    enforce(ssl_ !is null, new TSSLException("SSL_new: " ~
      getSSLErrorMessage()));

    SSL_set_fd(ssl_, socketHandle);
    int rc;
    if (serverSide_) {
      rc = SSL_accept(ssl_);
    } else {
      rc = SSL_connect(ssl_);
    }
    enforce(rc > 0, new TSSLException(getSSLErrorMessage(getErrno())));
    authorize();
  }

  void authorize() {
    alias TAccessManager.Decision Decision;

    auto rc = SSL_get_verify_result(ssl_);
    if (rc != X509_V_OK) {
      throw new TSSLException("SSL_get_verify_result(): " ~
        to!string(X509_verify_cert_error_string(rc)));
    }

    auto cert = SSL_get_peer_certificate(ssl_);
    if (cert is null) {
      // Certificate is not present.
      if (SSL_get_verify_mode(ssl_) & SSL_VERIFY_FAIL_IF_NO_PEER_CERT) {
        throw new TSSLException(
          "Authorize: Required certificate not present.");
      }

      // If we don't have an access manager set, we don't intend to authorize
      // the client, so everything's fine.
      if (serverSide_ && accessManager_ !is null) {
        throw new TSSLException(
          "Authorize: Certificate required for authorization.");
      }
      return;
    }

    if (accessManager_ is null) {
      // No access manager set, can return immediately as the cert is valid
      // and all peers are authorized.
      X509_free(cert);
      return;
    }

    // both certificate and access manager are present
    auto decision = accessManager_.verify(peerAddress);

    if (decision != Decision.SKIP) {
      X509_free(cert);
      if (decision != Decision.ALLOW) {
        throw new TSSLException("Authorize: Access denied based on remote IP.");
      }
      return;
    }

    // extract subjectAlternativeName
    string hostName;
    auto alternatives = X509_get_ext_d2i(cert, NID_subject_alt_name, null, null);
    if (alternatives != null) {
      auto count = sk_GENERAL_NAME_num(alternatives);
      for (int i = 0; decision == Decision.SKIP && i < count; i++) {
        auto name = sk_GENERAL_NAME_value(alternatives, i);
        if (name is null) {
          continue;
        }
        auto data = ASN1_STRING_data(name.d.ia5);
        auto length = ASN1_STRING_length(name.d.ia5);
        switch (name.type) {
          case GENERAL_NAME.GEN_DNS:
            if (hostName.empty) {
              hostName = (serverSide_ ? getPeerHost() : host);
            }
            decision = accessManager_.verify(host, cast(string)data[0 .. length]);
            break;
          case GENERAL_NAME.GEN_IPADD:
            decision = accessManager_.verify(peerAddress, cast(ubyte[])data[0 .. length]);
            break;
          default:
            // Do nothing.
        }
      }
      sk_GENERAL_NAME_pop_free(alternatives, GENERAL_NAME_free);
    }

    if (decision != Decision.SKIP) {
      X509_free(cert);
      if (decision != Decision.ALLOW) {
        throw new TSSLException("Authorize: Access denied.");
      }
      return;
    }

    // extract commonName
    auto name = X509_get_subject_name(cert);
    if (name !is null) {
      X509_NAME_ENTRY* entry;
      char* utf8;
      int last = -1;
      while (decision == Decision.SKIP) {
        last = X509_NAME_get_index_by_NID(name, NID_commonName, last);
        if (last == -1)
          break;
        entry = X509_NAME_get_entry(name, last);
        if (entry is null)
          continue;
        auto common = X509_NAME_ENTRY_get_data(entry);
        int size = ASN1_STRING_to_UTF8(&utf8, common);
        if (hostName.empty) {
          hostName = (serverSide_ ? getPeerHost() : host);
        }
        decision = accessManager_.verify(host, utf8[0 .. size].idup);
        CRYPTO_free(utf8);
      }
    }
    X509_free(cert);
    if (decision != Decision.ALLOW) {
      throw new TSSLException("Authorize: Cannot authorize peer.");
    }
  }

  bool serverSide_;
  SSL* ssl_;
  TSSLSocketFactory factory_;
  TAccessManager accessManager_;
}

/**
 * Creates SSL sockets and handles OpenSSL initialization/teardown.
 */
class TSSLSocketFactory {
  this() {
    initMutex_.lock();
    scope(exit) initMutex_.unlock();

    if (count_ == 0) {
      initializeOpenSSL();
      randomize();
    }

    count_++;
    context_ = SSLContext(SSL_CTX_new(TLSv1_method()));
  }

  ~this() {
    initMutex_.lock();
    scope(exit) initMutex_.unlock();
    count_--;
    if (count_ == 0) {
      cleanupOpenSSL();
    }
  }

  /**
   * Create an SSL socket wrapping an existing std.socket.Socket.
   *
   * Params:
   *   socket = An existing socket.
   */
  TSSLSocket createSocket(Socket socket) {
    auto result = new TSSLSocket(this, socket);
    setup(result);
    return result;
  }

   /**
   * Create an instance of TSSLSocket.
   *
   * Params:
   *   host = Remote host to connect to.
   *   port = Remote port to connect to.
   */
  TSSLSocket createSocket(string host, ushort port) {
    auto result = new TSSLSocket(this, host, port);
    setup(result);
    return result;
  }

  /**
   * Ciphers to be used in SSL handshake process.
   *
   * The string must be in the colon-delimited OpenSSL notation described in
   * ciphers(1), for example: "ALL:!ADH:!LOW:!EXP:!MD5:@STRENGTH".
   */
  void ciphers(string enable) @property {
    auto rc = SSL_CTX_set_cipher_list(context_.ctx, toStringz(enable));

    enforce(ERR_peek_error() == 0,
      new TSSLException("SSL_CTX_set_cipher_list: " ~ getSSLErrorMessage()));
    enforce(rc > 0, new TSSLException("None of specified ciphers are supported"));
  }

  /**
   * Whether peer is required to present a valid certificate.
   */
  void authenticate(bool required) @property {
    int mode;
    if (required) {
      mode = SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT |
        SSL_VERIFY_CLIENT_ONCE;
    } else {
      mode = SSL_VERIFY_NONE;
    }
    SSL_CTX_set_verify(context_.ctx, mode, null);
  }

  /**
   * Load server certificate.
   *
   * Params:
   *   path = Path to the certificate file.
   *   format = Certificate file format. Defaults to PEM, which is currently
   *     the only one supported.
   */
  void loadCertificate(string path, string format = "PEM") {
    enforce(path !is null && format !is null, new TTransportException(
      "loadCertificateChain: either <path> or <format> is null",
      TTransportException.Type.BAD_ARGS));

    if (format == "PEM") {
      if (SSL_CTX_use_certificate_chain_file(context_.ctx, toStringz(path)) == 0) {
        throw new TSSLException(`Could not load SSL server certificate ` ~
          `from file "` ~ path ~ `": ` ~ getSSLErrorMessage(getErrno()));
      }
    } else {
      throw new TSSLException("Unsupported certificate format: " ~ format);
    }
  }

  /*
   * Load private key.
   *
   * Params:
   *   path = Path to the certificate file.
   *   format = Private key file format. Defaults to PEM, which is currently
   *     the only one supported.
   */
  void loadPrivateKey(string path, string format = "PEM") {
    enforce(path !is null && format !is null, new TTransportException(
      "loadPrivateKey: either <path> or <format> is NULL",
      TTransportException.Type.BAD_ARGS));

    if (format == "PEM") {
       if (SSL_CTX_use_PrivateKey_file(context_.ctx, toStringz(path),
          SSL_FILETYPE_PEM) == 0)
        {
        throw new TSSLException(`Could not load SSL private key from file "` ~
          path ~ `": ` ~ getSSLErrorMessage(getErrno()));
      }
    } else {
      throw new TSSLException("Unsupported certificate format: " ~ format);
    }
  }

  /**
   * Load trusted certificates from specified file (in PEM format).
   *
   * Params.
   *   path = Path to the file containing the trusted certificates.
   */
  void loadTrustedCertificates(string path) {
    enforce(path !is null, new TTransportException(
      "loadTrustedCertificates: <path> is NULL",
      TTransportException.Type.BAD_ARGS));

    if (SSL_CTX_load_verify_locations(context_.ctx, toStringz(path), null) == 0) {
      throw new TSSLException(`Could not load SSL trusted certificate list ` ~
        `from file "` ~ path ~ `": ` ~ getSSLErrorMessage(getErrno()));
    }
  }

  /**
   * Called during OpenSSL initialization to seed the OpenSSL entropy pool.
   *
   * Defaults to simply calling RAND_poll(), but it can be overwritten if a
   * different, perhaps more secure implementation is desired.
   */
  void randomize() {
    RAND_poll();
  }

  /**
   * Whether to use client or server side SSL handshake protocol.
   */
  bool serverSide() @property const {
    return serverSide_;
  }

  /// Ditto
  void serverSide(bool value) @property {
    serverSide_ = value;
  }

  /**
   * The access manager to use.
   */
  TAccessManager accessManager() @property {
    return accessManager_;
  }

  /// Ditto
  void accessManager(TAccessManager value) @property {
    accessManager_ = value;
  }

protected:
  /**
   * Override this method for custom password callback. It may be called
   * multiple times at any time during a session as necessary.
   *
   * Params:
   *   size = Maximum length of password, including null byte.
   */
  string getPassword(int size) nothrow out(result) {
    assert(result.length < size);
  } body {
    return "";
  }

  /**
   * Notifies OpenSSL to use getPassword() instead of the default password
   * callback with getPassword().
   */
  void overrideDefaultPasswordCallback() {
    SSL_CTX_set_default_passwd_cb(context_.ctx, &passwordCallback);
    SSL_CTX_set_default_passwd_cb_userdata(context_.ctx, cast(void*)this);
  }

  SSLContext context_;

private:
  void setup(TSSLSocket ssl) {
    ssl.serverSide = serverSide;
    if (accessManager_ is null && !serverSide) {
      accessManager_ = new TDefaultClientAccessManager;
    }
    if (accessManager_ !is null) {
      ssl.accessManager = accessManager_;
    }
  }

  bool serverSide_;
  TAccessManager accessManager_;

  shared static this() {
    initMutex_ = new Mutex();
  }

  static void initializeOpenSSL() {
    if (initialized_) {
      return;
    }
    initialized_ = true;

    SSL_library_init();
    SSL_load_error_strings();

    mutexes_ = new Mutex[CRYPTO_num_locks()];
    foreach (ref m; mutexes_) {
      m = new Mutex;
    }

    // As per the OpenSSL threads manpage, this isn't needed on Windows.
    version (Posix) {
      CRYPTO_set_id_callback(&threadIdCallback);
    }
    CRYPTO_set_locking_callback(&lockingCallback);
    CRYPTO_set_dynlock_create_callback(&dynlockCreateCallback);
    CRYPTO_set_dynlock_lock_callback(&dynlockLockCallback);
    CRYPTO_set_dynlock_destroy_callback(&dynlockDestroyCallback);
  }

  static void cleanupOpenSSL() {
    if (!initialized_) return;

    initialized_ = false;
    CRYPTO_set_locking_callback(null);
    CRYPTO_set_dynlock_create_callback(null);
    CRYPTO_set_dynlock_lock_callback(null);
    CRYPTO_set_dynlock_destroy_callback(null);
    CRYPTO_cleanup_all_ex_data();
    ERR_free_strings();
    ERR_remove_state(0);
  }

  static extern(C) {
    version (Posix) {
      import core.sys.posix.pthread : pthread_self;
      c_ulong threadIdCallback() {
        return cast(c_ulong)pthread_self();
      }
    }

    void lockingCallback(int mode, int n, const(char)* file, int line) {
      if (mode & CRYPTO_LOCK) {
        mutexes_[n].lock();
      } else {
        mutexes_[n].unlock();
      }
    }

    CRYPTO_dynlock_value* dynlockCreateCallback(const(char)* file, int line) {
      enum size =  __traits(classInstanceSize, Mutex);
      auto mem = malloc(size)[0 .. size];
      if (!mem) onOutOfMemoryError();
      GC.addRange(mem.ptr, size);
      auto mutex = emplace!Mutex(mem);
      return cast(CRYPTO_dynlock_value*)mutex;
    }

    void dynlockLockCallback(int mode, CRYPTO_dynlock_value* l,
      const(char)* file, int line)
    {
      if (l is null) return;
      if (mode & CRYPTO_LOCK) {
        (cast(Mutex)l).lock();
      } else {
        (cast(Mutex)l).unlock();
      }
    }

    void dynlockDestroyCallback(CRYPTO_dynlock_value* l,
      const(char)* file, int line)
    {
      GC.removeRange(l);
      clear(cast(Mutex)l);
      free(l);
    }

    int passwordCallback(char* password, int size, int, void* data) nothrow {
      auto factory = cast(TSSLSocketFactory) data;
      auto userPassword = factory.getPassword(size);
      auto len = userPassword.length;
      if (len > size) {
        len = size;
      }
      password[0 .. len] = userPassword[0 .. len]; // TODO: \0 handling correct?
      return cast(int)len;
    }
  }

  static __gshared bool initialized_;
  static __gshared Mutex initMutex_;
  static __gshared Mutex[] mutexes_;
  static __gshared uint count_;
}

/**
 * Callback interface for access control. It's meant to verify the remote host.
 * It's constructed when application starts and set to TSSLSocketFactory
 * instance. It's passed onto all TSSLSocket instances created by this factory
 * object.
 */
class TAccessManager {
  ///
  enum Decision {
    DENY = -1, /// Deny access.
    SKIP =  0, /// Cannot decide, move on to next check (deny if last).
    ALLOW = 1  /// Allow access.
  }

  /**
   * Determine whether the peer should be granted access or not. It's called
   * once after the SSL handshake completes successfully, before peer certificate
   * is examined.
   *
   * If a valid decision (ALLOW or DENY) is returned, the peer certificate is
   * not to be verified.
   */
  Decision verify(InternetAddress sa) {
    return Decision.DENY;
  }

  /**
   * Determine whether the peer should be granted access or not. It's called
   * every time a DNS subjectAltName/common name is extracted from peer's
   * certificate.
   */
  Decision verify(string host, const(char)[] certHost) {
    return Decision.DENY;
  }

  /**
   * Determine whether the peer should be granted access or not. It's called
   * every time an IP subjectAltName is extracted from peer's certificate.
   */
  Decision verify(InternetAddress sa, ubyte[] certAddress) {
    return Decision.DENY;
  }
}


/**
 * Default access manager implementation, which just checks the host name
 * of the connection against the certificate.
 */
class TDefaultClientAccessManager : TAccessManager {
  override Decision verify(InternetAddress sa) {
    return Decision.SKIP;
  }

  override Decision verify(string host, const(char)[] certHost) {
    if (host.empty || certHost.empty) {
      return Decision.SKIP;
    }
    return (matchName(host, certHost) ? Decision.ALLOW : Decision.SKIP);
  }

  override Decision verify(InternetAddress sa, ubyte[] certAddress) {
    bool match;
    if (certAddress.length == sa.sizeof) {
      match = (cast(ubyte*)sa)[0 .. sa.sizeof] == certAddress[0 .. sa.sizeof];
    }
    return (match ? Decision.ALLOW : Decision.SKIP);
  }
}

private {
  /**
   * Match a name with a pattern. The pattern may include wildcard. A single
   * wildcard "*" can match up to one component in the domain name.
   *
   * Params:
   *   host = Host name to match, typically the SSL remote peer.
   *   pattern = Host name pattern, typically from the SSL certificate.
   *
   * Returns: true if host matches pattern, false otherwise.
   */
  bool matchName(const(char)[] host, const(char)[] pattern) {
    while (!host.empty && !pattern.empty) {
      if (toUpper(pattern.front) == toUpper(host.front)) {
        host.popFront;
        pattern.popFront;
      } else if (pattern.front == '*') {
        while (!host.empty && host.front != '.') {
          host.popFront;
        }
        pattern.popFront;
      } else {
        break;
      }
    }
    return (host.empty && pattern.empty);
  }

  unittest {
    assert(matchName("thrift.apache.org", "*.apache.org"));
    assert(!matchName("thrift.apache.org", "apache.org"));
    assert(matchName("thrift.apache.org", "thrift.*.*"));
    assert(matchName("", ""));
    assert(!matchName("", "*"));
  }
}

/**
 * SSL-level exception.
 */
class TSSLException : TTransportException {
  ///
  this(string msg, string file = __FILE__, size_t line = __LINE__,
    Throwable next = null)
  {
    super(msg, TTransportException.Type.INTERNAL_ERROR, file, line, next);
  }
}

private {
  string getSSLErrorMessage(int errnoCopy = 0) {
    string result;

    c_ulong code;
    while ((code = ERR_get_error()) != 0) {
      if (!result.empty) {
        result ~= ", ";
      }

      auto reason = ERR_reason_error_string(code);
      if (reason) {
        result ~= to!string(reason);
      } else {
        result ~= "SSL error #" ~ to!string(code);
      }
    }

    if (result.empty) {
      if (errnoCopy != 0) {
        result ~= to!string(strerror(errnoCopy));
      }
    }

    if (result.empty) {
      result ~= "Unknown error";
    }

    result ~= ".";

    return result;
  }

  /**
   * Wrap an OpenSSL SSL_CTX.
   */
  struct SSLContext {
    this(SSL_CTX* ctx) {
      ctx_ = ctx;
      enforce(ctx_ !is null, new TSSLException("SSL_CTX_new: " ~
        getSSLErrorMessage()));
      SSL_CTX_set_mode(ctx_, SSL_MODE_AUTO_RETRY);
    }

    ~this() {
      if (ctx_ !is null) {
        SSL_CTX_free(ctx_);
        ctx_ = null;
      }
    }

    SSL_CTX* ctx() @property {
      return ctx_;
    }

  private:
    @disable this(this) {
      assert(0);
    }

    SSL_CTX* ctx_;
  }
}
